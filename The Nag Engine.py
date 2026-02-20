import os
import logging
from datetime import datetime, timedelta
from supabase import create_client, Client

# --- CONFIGURATION ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") # MUST be service_role key

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("CRITICAL: Missing SUPABASE_URL or SUPABASE_KEY")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_expiring_policies(days_out=30):
    """Queries the vault for policies expiring within the danger zone."""
    target_date = (datetime.now() + timedelta(days=days_out)).date()
    today = datetime.now().date()
    
    logger.info(f"Scanning for policies expiring between {today} and {target_date}...")
    
    try:
        # Note: In a full production DB, we'd join with the 'vendors' table to get the email.
        # For the MVP, we assume we have an email or we pull it from the document metadata if needed.
        # For now, let's just find the expiring records.
        response = supabase.table("policies") \
            .select("*") \
            .eq("processing_status", "active") \
            .lte("expiration_date", target_date.isoformat()) \
            .gte("expiration_date", today.isoformat()) \
            .execute()
            
        return response.data
    except Exception as e:
        logger.error(f"Failed to query database: {e}")
        return []

def send_mock_email(policy):
    """Simulates sending a ruthless, polite compliance email."""
    carrier = policy.get("carrier_name", "Unknown Carrier")
    exp_date = policy.get("expiration_date")
    doc_id = policy.get("id")
    
    # In reality, this goes to the vendor's email address.
    target_email = "compliance@vendor.com" 
    
    subject = f"ACTION REQUIRED: Insurance Certificate Expiring - {carrier}"
    
    body = f"""
    To Whom It May Concern,
    
    Our records indicate that your Commercial General Liability policy ({carrier}) 
    will expire on {exp_date}.
    
    To maintain your active vendor status and avoid payment holds, please upload 
    your renewed Certificate of Insurance to our secure portal immediately.
    
    Portal Link: https://your-frontend-url.com
    Reference ID: {doc_id}
    
    This is an automated message from The Liability Shield.
    """
    
    logger.info(f"--- MOCK EMAIL SENT TO {target_email} ---")
    logger.info(f"Subject: {subject}")
    logger.info(body)
    logger.info("------------------------------------------")

def run_nag_cycle():
    logger.info("Starting Daily Nag Cycle...")
    expiring_policies = get_expiring_policies(days_out=30)
    
    if not expiring_policies:
        logger.info("Zero targets found. All vendors are compliant. Going back to sleep.")
        return
        
    logger.info(f"Found {len(expiring_policies)} non-compliant targets. Initiating email sequence.")
    
    for policy in expiring_policies:
        send_mock_email(policy)
        
    logger.info("Nag cycle complete.")

if __name__ == "__main__":
    run_nag_cycle()