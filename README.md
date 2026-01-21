# ETF Dividend Scraper

This project was created to test the abilities of [Claude Code](https://github.com/anthropics/claude-code) and experience the nirvana of vibe coding.

## What It Does

A web scraper that tracks dividend information for covered call ETFs, including:

- **YBTC** - Roundhill Bitcoin Covered Call Strategy ETF
- **BTCI** - Neos Bitcoin Covered Call ETF
- **QQQI** - Neos Nasdaq 100 High Income ETF
- **IWMI** - Neos Russell 2000 High Income ETF

## Stack

- **Scraper**: Python + Selenium
- **API**: Flask
- **Frontend**: HTML/JS
- **Deployment**: Docker

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the scraper
python scraper.py

# Start the API server
python api_server.py
```

Or with Docker:

```bash
docker-compose up
```

## License

MIT
