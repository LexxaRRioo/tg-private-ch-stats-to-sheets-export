# 📊 Telegram Stats Dashboard Bot

> Because staring at Telegram stats is more fun than staring at your code bugs 🎯

## 🎯 What Problem Does It Solve?

Managing multiple Telegram channels and chat communities can be challenging when you need to:
- Track engagement metrics over time
- Understand member growth patterns
- Analyze message frequency and peak activity times
- Monitor topic engagement in forum chats
- Make data-driven decisions about content strategy

This bot automates the collection of these metrics and presents them in an easy-to-analyze Google Sheets dashboard.

[See what you can get: BI Dashboard](https://datalens.yandex/vhix6u25akgwi)

## 🚀 Key Features

- **Channel Analytics:**
  - Member count tracking
  - Message volume monitoring
  - Basic word frequency analysis
  - Historical data collection
  
- **Forum Chat Analytics:**
  - Topic activity tracking
  - Hourly message distribution
  - First/last message tracking per topic

- **Automation:**
  - Hourly data collection via GitHub Actions
  - Incremental updates to avoid data duplication
  - Optional historical data backfill

- **Text Processing:**
  - Word normalization for Russian and English texts
  - Base form conversion (e.g., "running" -> "run", "бежал" -> "бежать")

## ⚡ Current Limitations

- Only processes the last 100 messages per channel in regular mode
- Requires user account authentication (not bot token)
- Limited to public channels and chats where the user is a member
- Maximum 100 topics per forum chat
- Rate limited by Telegram API (automatic retry handling implemented)

## 🛠️ Setup Guide

### Prerequisites

- Python 3.10+
- Google Sheets API access
- Telegram API credentials
- Google account

### Step-by-Step Installation

1. **Clone and prepare environment:**
```bash
git clone https://github.com/yourusername/tg-stats-dashboard-bot.git
cd tg-stats-dashboard-bot
python -m venv venv
source venv/bin/activate  # Linux/MacOS
.\venv\Scripts\activate   # Windows
pip install -r requirements.txt
```

2. **Get Telegram API Credentials:**
- Visit https://my.telegram.org/apps
- Create new application
- Save API_ID and API_HASH

3. **Set Up Google Sheets:**
- Create new Google Cloud project
- Enable Google Sheets API
- Create Service Account with Editor permissions
- Download credentials JSON
- Create new Google Sheet
- Share sheet with service account email (it's in the JSON)

See Resources section below for more detailed instruction.


4. **Configure Environment:**
- Copy `.env.example` to `.env`
- Fill in your credentials:
```env
TELEGRAM_API_ID=123456
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_CHANNELS={"channels":["https://t.me/channel1"],"chats":["https://t.me/chat1"]}
TG_SESSION=your_session_string (get in next step)
GOOGLE_SHEET_URL=your_sheet_url
GOOGLE_CREDENTIALS_PATH=path_to_credentials.json
TIMEZONE=Europe/Moscow
MODE=regular  # or 'backfill' for historical data
```

5. **Generate Telegram Session:**
```python
# Run this script once to get your session string
from telethon.sessions import StringSession
from telethon.sync import TelegramClient
import os
import load_dotenv

load_dotenv()
api_id = int(os.getenv("TELEGRAM_API_ID"))
api_hash = os.getenv("TELEGRAM_API_HASH")

with TelegramClient(StringSession(), api_id, api_hash) as client:
    print(client.session.save())
```
Add it to the .env


6. **For GitHub Actions:**
- Add all environment variables as repository secrets
- Ensure GOOGLE_CREDENTIALS contains the entire JSON content

## 📈 Usage

### Local Operation

Regular mode (last 24 hours):
```bash
python -m src.main
```

Historical data collection:
```bash
MODE=backfill START_DATE=2024-01-01 END_DATE=2024-01-31 python -m src.main
```

### Data Structure

The bot creates three sheets:

1. **channels_daily:**
   - Daily channel statistics
   - Member counts
   - Message volumes

2. **channel_messages:**
   - Message content analysis
   - Word frequency tracking

3. **chat_topics_hourly:**
   - Forum topic activity
   - Hourly message distribution

## 🌟 Ideas for Improvement

1. **Analytics Enhancement:**
   - Message view statistics
   - Reaction tracking
   - Media content analysis
   - User engagement patterns

2. **Technical Improvements:**
   - Bot token support
   - Message processing queue
   - Real-time alerts
   - Data validation layer
   - Word normalization for multiple languages (RU/EN)
     - Convert different word forms to base form
     - Add LANGUAGE_CODES env variable for supported languages
     - Integrate lemmatization libraries (e.g., pymorphy3 for Russian)
   
3. **Visualization:**
   - Built-in dashboard
   - Automated reports
   - Custom metrics

4. **Integration:**
   - Multiple dashboard options (Grafana, PowerBI)
   - Export to other formats
   - API endpoints

## 🤝 Contributing

Found a bug? Want to add a feature? Great! Please:

1. Fork the repository
2. Create feature branch (`git checkout -b amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin amazing-feature`)
5. Open Pull Request

## 📝 License

MIT License - feel free to use it as you wish, but don't blame us if your cat starts posting memes in your channels.

## 🔗 Resources

- [Example Dashboard](your_dashboard_link)
- [Telegram API Documentation](https://core.telegram.org/api)
- [Google Sheets API Guide](https://developers.google.com/sheets/api/guides/concepts)