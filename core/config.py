import os
from dotenv import load_dotenv
import logging

load_dotenv()

# Constants
BASE_URL = "https://docs.snowflake.com"
API_KEY = os.getenv("SPIDER_CLOUD_KEY", "sk-ce5e6935-7e74-49f8-affc-a90c55dad44a")
if not API_KEY:
    raise ValueError("SPIDER_CLOUD_KEY not found in environment variables")

# Snowflake connection parameters
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_WAREHOUSE   = os.getenv("SNOWFLAKE_WAREHOUSE")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)