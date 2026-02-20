import os
import json
import logging
import base64
import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from supabase import create_client, Client
from datetime import datetime
from nag_engine import run_nag_cycle

# --- CONFIGURATION ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") # MUST be service_role key
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("CRITICAL: Missing SUPABASE_URL or SUPABASE_KEY")

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"CRITICAL: Failed to init Supabase: {e}")

app = FastAPI(title="The Liability Shield - The Eye")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- DATA MODELS ---
class WebhookPayload(BaseModel):
    type: str
    table: str
    record: dict
    schema_name: str = Field(alias="schema", default="public") 
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

async def extract_data_with_gemini_raw(file_bytes, file_path: str):
    """Sends file to Gemini via raw HTTP to bypass broken SDKs."""
    mime_type = "application/pdf"
    lower_path = file_path.lower()
    if lower_path.endswith(".png"):
        mime_type = "image/png"
    elif lower_path.endswith((".jpg", ".jpeg")):
        mime_type = "image/jpeg"

    # Use the proven fallback loop
    target_models = [
        "gemini-2.5-flash",
        "gemini-2.0-flash",
        "gemini-1.5-flash"
    ]
    
    b64_data = base64.b64encode(file_bytes).decode('utf-8')
    headers = {"Content-Type": "application/json"}
    
    prompt_text = """
    Analyze this Certificate of Insurance (COI) document. 
    Extract the following data strictly in JSON format. Do not use markdown formatting.
    
    Keys to extract:
    - producer_name (Insurance Broker)
    - insured_name (Vendor)
    - insurer_name (Carrier Name)
    - policy_expiration_date (YYYY-MM-DD format ONLY. If multiple, take the earliest general liability expiration.)
    - general_liability_limit (Number only, no commas or currency symbols)
    - confidence_score (Float between 0.0 and 1.0)

    If a field is missing or illegible, return null.
    """
    
    payload = {
        "contents": [{
            "parts": [
                {"text": prompt_text},
                {"inline_data": {"mime_type": mime_type, "data": b64_data}}
            ]
        }]
    }

    async with httpx.AsyncClient() as client:
        last_error = None
        for model_name in target_models:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
            try:
                response = await client.post(url, headers=headers, json=payload, timeout=60.0)
                if response.status_code == 404:
                    continue
                
                response.raise_for_status()
                result = response.json()
                
                text = result['candidates'][0]['content']['parts'][0]['text']
                clean_text = text.replace("```json", "").replace("```", "").strip()
                logger.info(f"SUCCESS: Extracted using {model_name}")
                return json.loads(clean_text)

            except Exception as e:
                last_error = str(e)
                logger.warning(f"Failed with {model_name}: {last_error}")
                
        raise ValueError(f"All models failed. Last error: {last_error}")

@app.post("/webhook/process-coi")
async def process_coi_webhook(payload: WebhookPayload):
    logger.info(f"Received webhook for Policy ID: {payload.record.get('id')}")
    
    policy_id = payload.record['id']
    document_url = payload.record['document_url']
    
    try:
        file_bytes = download_file_from_supabase(document_url)
        extracted_data = await extract_data_with_gemini_raw(file_bytes, document_url)
        logger.info(f"Extraction result: {extracted_data}")
        
        # --- THE HARDENED BOUNCER ---
        exp_date_str = extracted_data.get("policy_expiration_date")
        status = "active"
        
        if exp_date_str:
            try:
                # Safely attempt to parse the date
                exp_date = datetime.strptime(exp_date_str, "%Y-%m-%d").date()
                if exp_date < datetime.now().date():
                    status = "rejected" # Expired
            except ValueError:
                logger.error(f"AI hallucinated date format: {exp_date_str}")
                status = "error" # Format was wrong, mark as error so human can review
        else:
            logger.error("AI returned null for expiration date.")
            status = "error" 
            
        update_data = {
            "carrier_name": extracted_data.get("insurer_name"),
            "expiration_date": exp_date_str if status != "error" else None,
            "limit_amount": extracted_data.get("general_liability_limit"),
            "ocr_confidence_score": extracted_data.get("confidence_score"),
            "ocr_data": extracted_data,
            "processing_status": status
        }
        
        supabase.table("policies").update(update_data).eq("id", policy_id).execute()
        return {"status": "success", "policy_status": status}

    except Exception as e:
        logger.error(f"Processing failed: {e}")
        supabase.table("policies").update({"processing_status": "error"}).eq("id", policy_id).execute()
        return {"status": "error", "message": str(e)}

@app.get("/trigger-nag")
def trigger_nag():
    """Hidden switch to wake the Nag Engine from the web."""
    try:
        run_nag_cycle()
        return {"status": "success", "message": "Assassin awake. Cycle executed. Check Render logs."}
    except Exception as e:
        logger.error(f"Nag Engine failed to execute: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/")
def health_check():
    return {"status": "awake", "system": "The Liability Shield"}
