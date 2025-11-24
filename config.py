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
    
    # ULTRA SPEED Settings
    MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2GB
    PRESIGNED_URL_EXPIRY = 3600  # 1 hour
    
    # MAXIMUM SPEED OPTIMIZATIONS
    MAX_WORKERS = 20  # Maximum workers for parallel processing
    CHUNK_SIZE = 50 * 1024 * 1024  # 50MB chunks for maximum throughput
    MULTIPART_THRESHOLD = 50 * 1024 * 1024  # 50MB
    MAX_CONCURRENCY = 20  # Maximum concurrent connections
    
    # Network optimizations - ULTRA AGGRESSIVE
    SOCKET_TIMEOUT = 60
    CONNECT_TIMEOUT = 15
    READ_TIMEOUT = 60
    RETRIES = 1  # Fewer retries for speed

config = Config()
