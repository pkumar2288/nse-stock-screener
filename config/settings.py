"""
Configuration settings for NSE Stock Screener
Set RUN_MODE = "cloud" for GitHub Actions, "local" for local execution
"""

import os

# ============= RUN MODE =============
# Set this to "cloud" when running on GitHub Actions
# Set this to "local" when running on your computer
RUN_MODE = os.environ.get("RUN_MODE", "local")  # cloud or local

# ============= TELEGRAM CONFIGURATION =============
# For GitHub: Set these as Repository Secrets
# For Local: Set as environment variables or hardcode (not recommended)
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ============= SCREENING PARAMETERS =============
ROUND_PRECI = 2
LOOKBACK_WEEKS = 15
THRESHOLD = 0.01      # 1% proximity for volatility levels
ODD_SQUARE_TOLERANCE = 0.5   # 0.5% proximity for odd-square filter
RSI_PERIOD = 14

# ============= BATCH PROCESSING =============
MAX_WORKERS = 10
BATCH_SIZE = 50

# ============= FILE HANDLING =============
# When RUN_MODE = "cloud", files are not saved locally
# They are created in memory and sent directly to Telegram
SAVE_LOCAL_FILES = (RUN_MODE == "cloud")
