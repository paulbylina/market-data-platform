import os
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

MASSIVE_BASE_URL = "https://api.massive.com"
MASSIVE_API_KEY = os.getenv("MASSIVE_API_KEY")
REQUEST_TIMEOUT_SECONDS = 30

DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
STAGING_DATA_DIR = DATA_DIR / "staging"
CURATED_DATA_DIR = DATA_DIR / "curated"
QUALITY_DATA_DIR = DATA_DIR / "quality"
LOGS_DIR = PROJECT_ROOT / "logs"


