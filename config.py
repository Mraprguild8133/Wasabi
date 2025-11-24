import os

class Config:
    # Telegram API
    API_ID = int(os.environ.get("API_ID", "123456"))
    API_HASH = os.environ.get("API_HASH", "your_api_hash")
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "your_bot_token")
    
    # Wasabi/S3 Configuration
    WASABI_ACCESS_KEY = os.environ.get("WASABI_ACCESS_KEY", "your_wasabi_access_key")
    WASABI_SECRET_KEY = os.environ.get("WASABI_SECRET_KEY", "your_wasabi_secret_key")
    WASABI_BUCKET = os.environ.get("WASABI_BUCKET", "your_bucket_name")
    WASABI_REGION = os.environ.get("WASABI_REGION", "us-east-1")
    WASABI_ENDPOINT = f"https://s3.{WASABI_REGION}.wasabisys.com"
    
    # Speed Optimization Settings
    MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2GB
    PRESIGNED_URL_EXPIRY = 3600  # 1 hour
    
    # ULTRA SPEED OPTIMIZATIONS
    MAX_WORKERS = 8
    CHUNK_SIZE = 64 * 1024 * 1024  # 64MB chunks
    MULTIPART_THRESHOLD = 64 * 1024 * 1024  # 64MB
    MAX_CONCURRENCY = 10
    
    # Network optimizations
    SOCKET_TIMEOUT = 30
    CONNECT_TIMEOUT = 10
    READ_TIMEOUT = 30

config = Config()
