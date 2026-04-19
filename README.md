# NSE Stock Screener

Automated stock screener for NSE F&O stocks using Odd Square and Volatility Level strategies.

## Features
- Screens all NSE F&O stocks
- Odd Square pattern detection (±0.5%)
- Fibonacci-based volatility levels (±1%)
- Daily & Weekly RSI (Wilder's smoothing)
- Automatic Telegram notifications
- Scheduled runs at market open times

## Setup

### Local Installation
```bash
git clone https://github.com/YOUR_USERNAME/nse-stock-screener.git
cd nse-stock-screener
pip install -r requirements.txt

# Set environment variables
export TELEGRAM_BOT_TOKEN="your_token"
export TELEGRAM_CHAT_ID="your_chat_id"
export RUN_MODE="local"

# Run
cd src
python screener.py
