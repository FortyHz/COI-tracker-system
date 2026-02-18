import os
import json
import logging
import base64
import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from supabase import create_client, Client
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

if not GEMINI_API_KEY:
    print("CRITICAL: Missing GEMINI_API_KEY")

app = FastAPI(title="The Liability Shield - The Eye")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- DATA MODELS ---
class WebhookPayload(BaseModel):
    type: str
    table: str
    record: dict
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

async def extract_data_with_gemini_raw(file_bytes):
    """
    Sends file to Gemini via raw HTTP request.
    Implements a Fallback Loop to handle '404 Model Not Found' errors 
    caused by Google's API naming churn.
    """
    
    # The Hit List: Try these models in order until one works.
    target_models = [
        "gemini-1.5-flash",
        "gemini-1.5-flash-001",
        "gemini-1.5-flash-latest",
        "gemini-1.5-flash-002",
        "gemini-1.5-pro" # Last resort fallback
    ]
    
    # Prepare common payload elements
    b64_data = base64.b64encode(file_bytes).decode('utf-8')
    headers = {"Content-Type": "application/json"}
    
    prompt_text = """
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
    
    payload = {
        "contents": [{
            "parts": [
                {"text": prompt_text},
                {
                    "inline_data": {
                        "mime_type": "application/pdf",
                        "data": b64_data
                    }
                }
            ]
        }]
    }

    async with httpx.AsyncClient() as client:
        last_error = None
        
        for model_name in target_models:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
            logger.info(f"Attempting extraction with model: {model_name}")
            
            try:
                response = await client.post(url, headers=headers, json=payload, timeout=60.0)
                
                # If 404, log and try next model
                if response.status_code == 404:
                    logger.warning(f"Model {model_name} not found (404). Retrying next...")
                    continue
                
                # If other error, raise it
                response.raise_for_status()
                result = response.json()
                
                # Parse Success
                try:
                    text = result['candidates'][0]['content']['parts'][0]['text']
                    clean_text = text.replace("```json", "").replace("```", "").strip()
                    logger.info(f"SUCCESS: Extracted using {model_name}")
                    return json.loads(clean_text)
                except (KeyError, IndexError, json.JSONDecodeError) as e:
                    logger.error(f"Failed to parse Gemini JSON from {model_name}: {result}")
                    raise ValueError(f"Invalid JSON structure: {e}")

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP Error on {model_name}: {e.response.text}")
                last_error = e
            except Exception as e:
                logger.error(f"Connection Error on {model_name}: {e}")
                last_error = e
        
        # If we exit the loop, nothing worked
        raise ValueError(f"All model attempts failed. Last error: {last_error}")

@app.post("/webhook/process-coi")
async def process_coi_webhook(payload: WebhookPayload):
    logger.info(f"Received webhook for Policy ID: {payload.record.get('id')}")
    
    record = payload.record
    policy_id = record['id']
    document_url = record['document_url']
    
    try:
        # Download
        file_bytes = download_file_from_supabase(document_url)
        
        # Extract (Now with Fallback Loop)
        extracted_data = await extract_data_with_gemini_raw(file_bytes)
        logger.info(f"Extraction Success: {extracted_data}")
        
        # Validate Dates
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
            
        # Update DB
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
