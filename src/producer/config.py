import logging
import os

from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

BASEURL = "alpha-vantage.p.rapidapi.com"
url = f"https://{BASEURL}/query"
api_key = os.getenv("API_KEY")
headers = {
    "x-rapidapi-key": api_key,
    "x-rapidapi-host": BASEURL
}

# Stocks to track — add or remove symbols here when expanding coverage
VALID_SYMBOLS = os.getenv("VALID_SYMBOLS", "TSLA,MSFT,GOOGL").split(",")

