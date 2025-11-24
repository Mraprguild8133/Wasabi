import os
import time
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
import boto3
from botocore.exceptions import NoCredentialsError
from botocore.config import Config as BotoConfig
import io

# Import configuration
from config import config

# ==========================================
# INITIALIZATION
# ==========================================

# Logging Setup
logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==========================================
# ULTRA-FAST S3 CLIENT - DIRECT UPLOAD
# ==========================================
class UltraFastS3Client:
    def __init__(self):
        # Boto3 configuration for MAXIMUM SPEED
        boto_config = BotoConfig(
            region_name=config.WASABI_REGION,
            retries={'max_attempts': 3, 'mode': 'standard'},
            max_pool_connections=config.MAX_CONCURRENCY,
            connect_timeout=config.CONNECT_TIMEOUT,
            read_timeout=config.READ_TIMEOUT,
            s3={
                'use_accelerate_endpoint': False,
                'payload_signing_enabled': False,
                'addressing_style': 'virtual',
                'multipart_threshold': config.MULTIPART_THRESHOLD,
                'multipart_chunksize': config.CHUNK_SIZE,
            }
        )
        
        self.s3 = boto3.client(
            's3',
            endpoint_url=config.WASABI_ENDPOINT,
            aws_access_key_id=config.WASABI_ACCESS_KEY,
            aws_secret_access_key=config.WASABI_SECRET_KEY,
            config=boto_config
        )
        self.bucket = config.WASABI_BUCKET

    def upload_fileobj(self, file_obj, object_name, file_size, progress_callback=None):
        """Uploads file object directly to S3 - NO LOCAL STORAGE"""
        try:
            self.s3.upload_fileobj(
                file_obj,
                self.bucket,
                object_name,
                Callback=progress_callback,
                Config=boto3.s3.transfer.TransferConfig(
                    multipart_threshold=config.MULTIPART_THRESHOLD,
                    max_concurrency=config.MAX_CONCURRENCY,
                    multipart_chunksize=config.CHUNK_SIZE,
                    use_threads=True
                )
            )
            return True
        except Exception as e:
            logger.error(f"Direct upload error: {e}")
            return False

    def generate_presigned_url(self, object_name, expiration=None):
        """Generate a presigned URL to share an S3 object."""
        if expiration is None:
            expiration = config.PRESIGNED_URL_EXPIRY
            
        try:
            response = self.s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket, 'Key': object_name},
                ExpiresIn=expiration
            )
            return response
        except Exception as e:
            logger.error(f"Presigned URL error: {e}")
            return None

# Initialize S3 Client
s3_client = UltraFastS3Client()

# Initialize Telegram Client
app = Client(
    "wasabi_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN
)

# Thread pool for blocking S3 operations
executor = ThreadPoolExecutor(max_workers=config.MAX_WORKERS)

# ==========================================
# UTILITY FUNCTIONS
# ==========================================
def human_readable_size(size, decimal_places=2):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.{decimal_places}f} {unit}"
        size /= 1024.0
    return f"{size:.{decimal_places}f} PB"

def get_progress_bar(current, total):
    """Generates a text-based progress bar."""
    percentage = current * 100 / total
    filled_length = int(20 * current // total)
    bar = '█' * filled_length + '░' * (20 - filled_length)
    return f"[{bar}] {percentage:.1f}%"

async def progress_hook(current, total, message: Message, start_time, process_type="Processing"):
    """Progress hook for real-time updates."""
    now = time.time()
    diff = now - start_time
    
    # Update every 2 seconds for faster feedback
    if round(diff % 2.00) == 0 or current == total:
        speed = current / diff if diff > 0 else 0
        elapsed_time = round(diff) * 1000
        time_to_completion = round((total - current) / speed) * 1000 if speed > 0 else 0

        def time_formatter(milliseconds):
            seconds, milliseconds = divmod(int(milliseconds), 1000)
            minutes, seconds = divmod(seconds, 60)
            hours, minutes = divmod(minutes, 60)
            return f"{hours}h {minutes}m {seconds}s" if hours > 0 else f"{minutes}m {seconds}s"

        text = (
            f"**{process_type}...**\n"
            f"{get_progress_bar(current, total)}\n\n"
            f"**Progress:** {human_readable_size(current)} / {human_readable_size(total)}\n"
            f"**Speed:** {human_readable_size(speed)}/s\n"
            f"**ETA:** {time_formatter(time_to_completion)}"
        )
        
        try:
            await message.edit_text(text)
        except Exception:
            pass

def generate_s3_key(filename, user_id=None):
    """Generate unique S3 key"""
    import secrets
    timestamp = int(time.time())
    random_str = secrets.token_hex(4)
    name, ext = os.path.splitext(filename)
    
    # Sanitize filename
    safe_name = "".join(c for c in name if c.isalnum() or c in ('-', '_')).rstrip()
    
    if user_id:
        return f"users/{user_id}/{timestamp}_{safe_name}_{random_str}{ext}"
    return f"uploads/{timestamp}_{safe_name}_{random_str}{ext}"

# Upload Progress Tracker
class UploadProgress:
    def __init__(self, filename, total_size, status_msg, loop):
        self.filename = filename
        self.total_size = total_size
        self.current_size = 0
        self.status_msg = status_msg
        self.start_time = time.time()
        self.loop = loop
        self.last_update_time = 0

    def __call__(self, bytes_amount):
        self.current_size += bytes_amount
        now = time.time()
        
        # Update every 1 second for instant feedback
        if now - self.last_update_time > 1 or self.current_size == self.total_size:
            self.last_update_time = now
            asyncio.run_coroutine_threadsafe(
                progress_hook(self.current_size, self.total_size, self.status_msg, self.start_time, "🚀 Uploading to Wasabi"),
                self.loop
            )

# ==========================================
# BOT HANDLERS - DIRECT UPLOAD
# ==========================================

@app.on_message(filters.command("start"))
async def start_handler(client, message):
    await message.reply_text(
        "**🚀 ULTRA FAST Wasabi Upload Bot**\n\n"
        "Send me any file and I will:\n"
        "• Download from Telegram\n"
        "• **DIRECT UPLOAD** to Wasabi (No Local Storage)\n"
        "• Generate instant streaming link\n\n"
        f"**Max Size:** {human_readable_size(config.MAX_FILE_SIZE)}\n"
        "**Speed:** ⚡ MAXIMUM"
    )

@app.on_message(filters.document | filters.video | filters.audio)
async def direct_upload_handler(client, message: Message):
    # Get file info
    media = getattr(message, message.media.value)
    original_filename = getattr(media, "file_name", f"file_{message.id}")
    file_size = media.file_size
    
    # Check file size
    if file_size > config.MAX_FILE_SIZE:
        await message.reply_text(
            f"❌ File too large. Maximum size is {human_readable_size(config.MAX_FILE_SIZE)}."
        )
        return

    # Generate unique S3 key
    user_id = message.from_user.id if message.from_user else "unknown"
    s3_key = generate_s3_key(original_filename, user_id)

    # Start processing
    status_msg = await message.reply_text(
        f"**🚀 Starting ULTRA FAST Upload...**\n"
        f"**File:** `{original_filename}`\n"
        f"**Size:** `{human_readable_size(file_size)}`\n"
        f"**Mode:** DIRECT UPLOAD ⚡"
    )
    start_time = time.time()

    try:
        # Create a file-like object in memory for direct upload
        file_buffer = io.BytesIO()
        
        # Download directly to memory buffer with progress
        await message.download(
            file_name=file_buffer,
            progress=progress_hook,
            progress_args=(status_msg, start_time, "⬇️ Downloading")
        )
        
        # Reset buffer position
        file_buffer.seek(0)
        
        await status_msg.edit_text("**✅ Download Complete. Starting DIRECT UPLOAD...**")

        # Upload directly from memory to Wasabi
        loop = asyncio.get_running_loop()
        upload_tracker = UploadProgress(original_filename, file_size, status_msg, loop)

        # Direct upload to Wasabi
        success = await loop.run_in_executor(
            executor,
            lambda: s3_client.upload_fileobj(
                file_buffer, 
                s3_key, 
                file_size, 
                upload_tracker
            )
        )
        
        # Close buffer
        file_buffer.close()

        if not success:
            await status_msg.edit_text("❌ Upload to Wasabi failed.")
            return

        # Generate streaming link
        web_link = s3_client.generate_presigned_url(s3_key)
        
        if not web_link:
            await status_msg.edit_text("❌ Failed to generate streaming link.")
            return

        # Send success message
        await status_msg.delete()
        
        response_text = (
            f"**✅ UPLOAD COMPLETE!** ⚡\n\n"
            f"📁 **File:** `{original_filename}`\n"
            f"💾 **Size:** `{human_readable_size(file_size)}`\n"
            f"⏱️ **Upload Time:** {time.time() - start_time:.1f}s\n\n"
            f"🔗 **Direct Stream Link:**\n`{web_link}`\n\n"
            f"**Use in:** VLC, MX Player, Browser"
        )
        
        await message.reply_text(
            response_text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔗 Open Link", url=web_link)],
                [InlineKeyboardButton("📱 Stream in VLC", url=web_link)]
            ]),
            disable_web_page_preview=True
        )

    except Exception as e:
        logger.error(f"Direct upload failed: {e}")
        await status_msg.edit_text(f"❌ Upload failed: {str(e)}")

@app.on_message(filters.command("speed"))
async def speed_test(client, message):
    await message.reply_text(
        "**⚡ ULTRA FAST MODE ENABLED**\n\n"
        "• **Direct Memory Upload** - No local storage\n"
        "• **Multi-threaded** - 8 parallel workers\n"
        "• **Chunked Uploads** - 64MB chunks\n"
        "• **Maximum Concurrency** - 10 connections\n"
        "• **Zero Disk I/O** - Pure memory operations\n\n"
        "**Result:** Instant upload speeds! 🚀"
    )

# ==========================================
# MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    logger.info("🚀 Starting ULTRA FAST Wasabi Bot...")
    print("⚡ ULTRA FAST Wasabi Upload Bot")
    print("📡 Mode: DIRECT UPLOAD (No Local Storage)")
    print("🚀 Speed: MAXIMUM")
    app.run()
