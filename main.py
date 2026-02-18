import os
import json
import logging
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from supabase import create_client, Client
import google.generativeai as genai
from datetime import datetime

# --- CONFIGURATION ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("CRITICAL: Missing SUPABASE_URL or SUPABASE_KEY")

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"CRITICAL: Failed to init Supabase: {e}")

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
else:
    print("CRITICAL: Missing GEMINI_API_KEY")

app = FastAPI(title="The Liability Shield - The Eye")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- DATA MODELS ---
class WebhookPayload(BaseModel):
    type: str
    table: str
    record: dict
    # Fix: 'schema' shadows a Pydantic attribute, so we map it from the JSON key 'schema' to a python variable 'schema_name'
    schema_name: str = Field(alias="schema") 
    old_record: dict | None = None

# --- THE LOGIC ---

def download_file_from_supabase(file_path: str):
    try:
        clean_path = file_path.replace("cois/", "")
        logger.info(f"Downloading {clean_path} from bucket 'cois'...")
        data = supabase.storage.from_("cois").download(clean_path)
        return data
    except Exception as e:
        logger.error(f"Failed to download file: {e}")
        raise HTTPException(status_code=500, detail="Storage Download Failed")

def extract_data_with_gemini(file_bytes):
    # FIX: Use the specific, stable model version
    model = genai.GenerativeModel("gemini-1.5-flash-001")
    
    prompt = """
    You are a strictly logical Data Extraction Engine.
    Analyze this Certificate of Insurance (COI) PDF. 
    
    Extract the following data strictly in JSON format. Do not include Markdown formatting (no ```json ... ```).
    
    Keys to extract:
    - producer_name (Insurance Broker)
    - insured_name (Vendor Company Name)
    - insurer_name (The Main Carrier Name)
    - policy_expiration_date (YYYY-MM-DD format. Look for General Liability or Umbrella expiration. If multiple, take the earliest one.)
    - general_liability_limit (Number only, remove currency symbols and commas. e.g. 1000000)
    - confidence_score (Float between 0.0 and 1.0. 1.0 = clear text, 0.1 = blurry/illegible)

    If a field is missing or illegible, return null.
    """
    
    try:
        response = model.generate_content([
            {"mime_type": "application/pdf", "data": file_bytes},
            prompt
        ])
        
        text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        logger.error(f"Gemini Extraction Failed: {e}")
        raise HTTPException(status_code=500, detail=f"AI Extraction Failed: {str(e)}")

@app.post("/webhook/process-coi")
async def process_coi_webhook(payload: WebhookPayload):
    logger.info(f"Received webhook for Policy ID: {payload.record.get('id')}")
    
    record = payload.record
    policy_id = record['id']
    document_url = record['document_url']
    
    try:
        file_bytes = download_file_from_supabase(document_url)
        extracted_data = extract_data_with_gemini(file_bytes)
        logger.info(f"Extraction Success: {extracted_data}")
        
        exp_date_str = extracted_data.get("policy_expiration_date")
        status = "active"
        
        if exp_date_str:
            try:
                exp_date = datetime.strptime(exp_date_str, "%Y-%m-%d").date()
                if exp_date < datetime.now().date():
                    status = "rejected"
            except ValueError:
                status = "error"
        else:
            status = "error"
            
        update_data = {
            "carrier_name": extracted_data.get("insurer_name"),
            "policy_number": "PENDING",
            "expiration_date": exp_date_str if exp_date_str else None,
            "limit_amount": extracted_data.get("general_liability_limit"),
            "ocr_confidence_score": extracted_data.get("confidence_score"),
            "ocr_data": extracted_data,
            "processing_status": status
        }
        
        supabase.table("policies").update(update_data).eq("id", policy_id).execute()
        return {"status": "success", "policy_status": status}

    except Exception as e:
        logger.error(f"Processing CRITICAL FAILURE: {e}")
        try:
            supabase.table("policies").update({"processing_status": "error"}).eq("id", policy_id).execute()
        except:
            pass
        return {"status": "error", "message": str(e)}

@app.get("/")
def health_check():
    return {"status": "awake", "system": "The Liability Shield"}
