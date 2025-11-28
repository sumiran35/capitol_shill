import os
from dotenv import load_dotenv

# Load environment variables from a.env file
load_dotenv()

FMP_API_KEY = os.getenv("FMP_API_KEY")
HF_TOKEN = os.getenv("HF_TOKEN")

if not FMP_API_KEY:
    raise ValueError("FMP_API_KEY is missing. Please check your environment configuration.")