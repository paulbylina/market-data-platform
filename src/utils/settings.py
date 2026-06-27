import os
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"

RAW_DATA_DIR = DATA_DIR / "raw"
STANDARDIZED_DATA_DIR = DATA_DIR / "standardized"
CURATED_DATA_DIR = DATA_DIR / "curated"
QUALITY_DATA_DIR = DATA_DIR / "quality"
SERVING_DATA_DIR = DATA_DIR / "serving"

LOGS_DIR = PROJECT_ROOT / "logs"

MASSIVE_BASE_URL = "https://api.massive.com"
REQUEST_TIMEOUT_SECONDS = 30

MASSIVE_API_KEY = os.getenv("MASSIVE_API_KEY")