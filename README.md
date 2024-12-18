# Telegram Stats Dashboard Bot

Collects statistics from Telegram channels and chats, then sends them to Google Sheets for analysis. Features include:

- Channel member count tracking
- Message volume analytics
- Chat topic activity monitoring
- Word frequency analysis
- Automated hourly data collection via GitHub Actions

## Setup

1. Clone repository
```bash
git clone https://github.com/yourusername/tg-stats-dashboard-bot.git
cd tg-stats-dashboard-bot
```

2. Create virtual environment
```bash
python -m venv venv
source venv/bin/activate  # Linux/MacOS
venv\Scripts\activate     # Windows
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

4. Generate Telegram session string (yes, it is required to input your phone number, code and password once, script uses your user as an entrypoint and does its best to not violate PoS and do not block your user):
```python
from telethon.sessions import StringSession
from telethon.sync import TelegramClient
import os
from dotenv import load_dotenv 

load_dotenv()
api_id = int(os.getenv("TELEGRAM_API_ID"))
api_hash = os.getenv("TELEGRAM_API_HASH")

with TelegramClient(StringSession(), api_id, api_hash) as client:
    print(client.session.save())
```

5. Configure credentials:
   - Copy `.env.example` to `.env`
   - Get Telegram API credentials from https://my.telegram.org/apps
   - Set up Google Sheets API ([detailed instruction](https://medium.com/@a.marenkov/how-to-get-credentials-for-google-sheets-456b7e88c430)):
     1. Go to Google Cloud Console
     2. Create new project
     3. Enable Google Sheets API
     4. Create Service Account
     5. Download JSON credentials
     6. Share target Google Sheet with service account email

6. Configure `.env`:
```bash
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_CHANNELS={"channels":["https://t.me/channel1"],"chats":["https://t.me/chat1"]}
GOOGLE_SHEET_URL=your_sheet_url
GOOGLE_CREDENTIALS_PATH=path_to_credentials.json
TIMEZONE=Europe/Moscow
```

## Usage

### Local Run
```bash
python -m src.main
```

### GitHub Actions Setup

1. Add repository secrets:
   - TELEGRAM_API_ID
   - TELEGRAM_API_HASH
   - TELEGRAM_CHANNELS
   - GOOGLE_SHEET_URL
   - GOOGLE_CREDENTIALS (full JSON content)
   - TG_SESSION (from step 4 of Setup)

2. Actions will run hourly automatically

### Backfill Mode

To collect historical data:
```bash
MODE=backfill
```

## Data Structure

### Sheets:

1. channels_daily
   - Channel stats
   - Member counts
   - Message volumes

2. channel_messages
   - Message content analysis
   - Word frequency

3. chat_topics_hourly
   - Topic activity
   - Message counts per hour

## Contributing

1. Fork repository
2. Create feature branch
3. Submit pull request

## License

MIT License