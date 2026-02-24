import os
import logging
from datetime import datetime, timedelta
from supabase import create_client, Client
import resend

# --- CONFIGURATION ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") # MUST be service_role key
RESEND_API_KEY = os.environ.get("RESEND_API_KEY")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("CRITICAL: Missing SUPABASE_URL or SUPABASE_KEY")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Arm the weapon if the key is present
if RESEND_API_KEY:
    resend.api_key = RESEND_API_KEY
else:
    logger.warning("No RESEND_API_KEY found. Engine will run in MOCK mode (firing blanks).")

def get_expiring_policies(days_out=30):
    """Queries the vault for policies expiring within the danger zone."""
    target_date = (datetime.now() + timedelta(days=days_out)).date()
    today = datetime.now().date()
    
    logger.info(f"Scanning for policies expiring between {today} and {target_date}...")
    
    try:
        # STEP 2: TARGET MAPPING 
        # The 'vendors(company_name, contact_email)' syntax tells Supabase to follow 
        # the foreign key and grab the actual human's email attached to this document.
        response = supabase.table("policies") \
            .select("*, vendors(company_name, contact_email)") \
            .eq("processing_status", "active") \
            .lte("expiration_date", target_date.isoformat()) \
            .gte("expiration_date", today.isoformat()) \
            .execute()
            
        return response.data
    except Exception as e:
        logger.error(f"Failed to query database: {e}")
        return []

def send_email(policy):
    """Fires a lethal or mock email depending on environment configuration."""
    carrier = policy.get("carrier_name", "Unknown Carrier")
    exp_date = policy.get("expiration_date")
    doc_id = policy.get("id")
    
    # Extract vendor data if it exists (Target Mapping)
    vendor_data = policy.get("vendors")
    if vendor_data and vendor_data.get("contact_email"):
        target_email = vendor_data.get("contact_email")
        vendor_name = vendor_data.get("company_name")
    else:
        # FALLBACK: If you upload a test PDF without linking a vendor, it shoots here.
        target_email = "mlodic.blue@gmail.com" 
        vendor_name = "Valued Vendor"
    
    # THE COMPASS FIX: Pointing to the live Render UI
    frontend_url = "https://the-liability-shield-face.onrender.com"
    
    subject = f"ACTION REQUIRED: Insurance Certificate Expiring - {carrier}"
    
    body = f"""
    Dear {vendor_name},
    
    Our records indicate that your Commercial General Liability policy ({carrier}) 
    will expire on {exp_date}.
    
    To maintain your active vendor status and avoid payment holds, please upload 
    your renewed Certificate of Insurance to our secure portal immediately.
    
    Portal Link: {frontend_url}
    Reference ID: {doc_id}
    
    Instructions:
    1. Click the portal link above.
    2. Click 'Continue with Google' to securely access your dashboard.
    3. Upload your renewed PDF document.
    
    This is an automated message from The Liability Shield.
    """
    
    # STEP 1: THE AMMO
    if RESEND_API_KEY:
        try:
            # Resend requires a verified domain. However, for testing, 
            # they allow you to send from "onboarding@resend.dev" to the email you signed up with.
            r = resend.Emails.send({
                "from": "onboarding@resend.dev",
                "to": target_email,
                "subject": subject,
                "text": body
            })
            logger.info(f"LIVE ROUND FIRED: Email sent to {target_email}. Resend ID: {r.get('id')}")
        except Exception as e:
            logger.error(f"Misfire! Resend API rejected the email: {e}")
    else:
        logger.info(f"--- MOCK EMAIL SENT TO {target_email} ---")
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
        send_email(policy)
        
    logger.info("Nag cycle complete.")

if __name__ == "__main__":
    run_nag_cycle()