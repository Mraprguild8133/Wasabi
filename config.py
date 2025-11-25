import os

class Config:
    # Telegram API
  TOKEN = os.environ.get("BOT_TOKEN", "your_bot_token")

# Database file path
DB_FILE = "bot_database.db"

# Flood control settings
FLOOD_LIMIT = 5  # messages
FLOOD_WINDOW = 5  # seconds

# Optional: other configuration items
# ADMIN_IDS = [123456789, 987654321]  # Admin ID list
# LOG_LEVEL = "INFO"
