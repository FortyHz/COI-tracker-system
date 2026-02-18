import os
import json
import logging
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from supabase import create_client, Client
import google.generativeai as genai
from datetime import datetime

# --- CONFIGURATION ---
# Load from Environment Variables (Set these in Render)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") # MUST be the SERVICE_ROLE key to write to DB
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# Initialize Clients
if not SUPABASE_URL or not SUPABASE_KEY:
    # Fail fast if env vars are missing
    print("CRITICAL: Missing SUPABASE_URL or SUPABASE_KEY")

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"CRITICAL: Failed to init Supabase: {e}")

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
else:
    print("CRITICAL: Missing GEMINI_API_KEY")

# Initialize App
app = FastAPI(title="The Liability Shield - The Eye")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- DATA MODELS ---
class WebhookPayload(BaseModel):
    # Matches the Supabase Database Webhook payload structure
    type: str
    table: str
    record: dict
    schema: str
    old_record: dict | None = None

# --- THE LOGIC ---

def download_file_from_supabase(file_path: str):
    """Downloads the file bytes from Supabase Storage."""
    try:
        # The frontend sends the file path relative to the bucket root (e.g., "0.123.pdf")
        # Ensure we are just using the filename if that's how it was uploaded
        clean_path = file_path.replace("cois/", "")
        
        logger.info(f"Downloading {clean_path} from bucket 'cois'...")
        data = supabase.storage.from_("cois").download(clean_path)
        return data
    except Exception as e:
        logger.error(f"Failed to download file: {e}")
        raise HTTPException(status_code=500, detail="Storage Download Failed")

def extract_data_with_gemini(file_bytes):
    """Sends file to Gemini 1.5 Flash for extraction."""
    # CHANGED: Use the specific -001 version to avoid 404s on some SDK versions
    # This is the fix for the "404 models/gemini-1.5-flash is not found" error
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
        
        # Clean the response (Gemini sometimes wraps in markdown blocks despite instructions)
        text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        logger.error(f"Gemini Extraction Failed: {e}")
        # Pass the specific error up so we can see it in logs
        raise HTTPException(status_code=500, detail=f"AI Extraction Failed: {str(e)}")

@app.post("/webhook/process-coi")
async def process_coi_webhook(payload: WebhookPayload):
    """
    Triggered by Supabase Database Webhook when a row is inserted into 'policies'.
    """
    logger.info(f"Received webhook for Policy ID: {payload.record.get('id')}")
    
    # 1. Parse Payload
    record = payload.record
    policy_id = record['id']
    document_url = record['document_url']
    
    # 2. Execution Logic
    try:
        # Download
        file_bytes = download_file_from_supabase(document_url)
        
        # Extract
        extracted_data = extract_data_with_gemini(file_bytes)
        logger.info(f"Extraction Success: {extracted_data}")
        
        # Validate Dates (The Bouncer)
        exp_date_str = extracted_data.get("policy_expiration_date")
        status = "active"
        
        if exp_date_str:
            try:
                exp_date = datetime.strptime(exp_date_str, "%Y-%m-%d").date()
                if exp_date < datetime.now().date():
                    status = "rejected" # Expired
            except ValueError:
                status = "error" # Bad date format
        else:
            status = "error" # Could not find date
            
        # 3. Update Database (The Vault)
        update_data = {
            "carrier_name": extracted_data.get("insurer_name"),
            "policy_number": "PENDING", # Add extraction for this if needed later
            "expiration_date": exp_date_str if exp_date_str else None,
            "limit_amount": extracted_data.get("general_liability_limit"),
            "ocr_confidence_score": extracted_data.get("confidence_score"),
            "ocr_data": extracted_data, # Store raw JSON for audit
            "processing_status": status
        }
        
        # Execute Update
        data = supabase.table("policies").update(update_data).eq("id", policy_id).execute()
        
        return {"status": "success", "policy_status": status, "data": data}

    except Exception as e:
        logger.error(f"Processing CRITICAL FAILURE: {e}")
        # Mark as Error in DB so it doesn't stay 'processing' forever
        try:
            supabase.table("policies").update({"processing_status": "error"}).eq("id", policy_id).execute()
        except:
            pass # If DB is down, we can't do much
        return {"status": "error", "message": str(e)}

@app.get("/")
def health_check():
    return {"status": "awake", "system": "The Liability Shield"}
