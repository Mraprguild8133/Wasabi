import os
import time
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
import boto3
from botocore.config import Config as BotoConfig
import tempfile
import aiohttp
import aiofiles

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
# ULTRA FAST S3 CLIENT
# ==========================================
class UltraFastS3Client:
    def __init__(self):
        # ULTRA AGGRESSIVE Boto3 configuration
        boto_config = BotoConfig(
            region_name=config.WASABI_REGION,
            retries={'max_attempts': config.RETRIES, 'mode': 'standard'},
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

    def upload_file(self, file_path, object_name):
        """ULTRA FAST file upload - no progress tracking for maximum speed"""
        try:
            # Use transfer config for maximum speed
            transfer_config = boto3.s3.transfer.TransferConfig(
                multipart_threshold=config.MULTIPART_THRESHOLD,
                max_concurrency=config.MAX_CONCURRENCY,
                multipart_chunksize=config.CHUNK_SIZE,
                use_threads=True
            )
            
            self.s3.upload_file(
                file_path,
                self.bucket,
                object_name,
                Config=transfer_config
            )
            return True
        except Exception as e:
            logger.error(f"Upload error: {e}")
            return False

    def generate_presigned_url(self, object_name, expiration=None):
        """Generate a presigned URL"""
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
    bot_token=config.BOT_TOKEN,
    # Pyrogram optimizations
    workers=100,
    max_concurrent_transmissions=10
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

def generate_s3_key(filename, user_id=None):
    """Generate unique S3 key"""
    import secrets
    timestamp = int(time.time())
    random_str = secrets.token_hex(4)
    name, ext = os.path.splitext(filename)
    
    safe_name = "".join(c for c in name if c.isalnum() or c in ('-', '_')).rstrip()
    
    if user_id:
        return f"users/{user_id}/{timestamp}_{safe_name}_{random_str}{ext}"
    return f"uploads/{timestamp}_{safe_name}_{random_str}{ext}"

# ==========================================
# ULTRA FAST UPLOAD SYSTEM
# ==========================================
class UltraFastUploader:
    def __init__(self):
        self.upload_speeds = []
    
    async def download_file(self, message, temp_path):
        """ULTRA FAST download without progress tracking"""
        try:
            await message.download(file_name=temp_path)
            return True
        except Exception as e:
            logger.error(f"Download error: {e}")
            return False
    
    async def upload_to_wasabi(self, file_path, s3_key):
        """ULTRA FAST upload to Wasabi"""
        try:
            loop = asyncio.get_running_loop()
            success = await loop.run_in_executor(
                executor,
                lambda: s3_client.upload_file(file_path, s3_key)
            )
            return success
        except Exception as e:
            logger.error(f"Upload error: {e}")
            return False

# ==========================================
# BOT HANDLERS - ULTRA FAST
# ==========================================

@app.on_message(filters.command("start"))
async def start_handler(client, message):
    await message.reply_text(
        "**🚀 ULTRA FAST Wasabi Upload Bot**\n\n"
        "• **INSTANT UPLOAD** - Maximum speed optimized\n"
        "• **No Progress Tracking** - Removed bottlenecks\n"
        "• **50MB Chunks** - Maximum throughput\n"
        "• **20 Concurrent Connections** - Full bandwidth usage\n\n"
        f"**Max Size:** {human_readable_size(config.MAX_FILE_SIZE)}\n"
        "**Speed:** 🚀 ULTRA FAST"
    )

@app.on_message(filters.document | filters.video | filters.audio)
async def ultra_fast_upload_handler(client, message: Message):
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

    # Start processing - SIMPLE STATUS
    status_msg = await message.reply_text(
        f"**🚀 STARTING ULTRA FAST UPLOAD...**\n"
        f"**File:** `{original_filename}`\n"
        f"**Size:** `{human_readable_size(file_size)}`\n"
        f"**Mode:** MAXIMUM SPEED"
    )
    
    start_time = time.time()
    uploader = UltraFastUploader()

    try:
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=True, suffix=".tmp") as temp_file:
            temp_path = temp_file.name

            # PHASE 1: ULTRA FAST DOWNLOAD
            await status_msg.edit_text("**⬇️ DOWNLOADING AT MAXIMUM SPEED...**")
            
            download_start = time.time()
            download_success = await uploader.download_file(message, temp_path)
            download_time = time.time() - download_start
            
            if not download_success:
                await status_msg.edit_text("❌ Download failed")
                return

            # PHASE 2: ULTRA FAST UPLOAD
            await status_msg.edit_text("**⬆️ UPLOADING TO WASABI AT MAXIMUM SPEED...**")
            
            upload_start = time.time()
            upload_success = await uploader.upload_to_wasabi(temp_path, s3_key)
            upload_time = time.time() - upload_start

            if not upload_success:
                await status_msg.edit_text("❌ Upload failed")
                return

        # Generate streaming link
        web_link = s3_client.generate_presigned_url(s3_key)
        
        if not web_link:
            await status_msg.edit_text("❌ Failed to generate streaming link")
            return

        # Calculate speeds
        total_time = time.time() - start_time
        download_speed = file_size / download_time if download_time > 0 else 0
        upload_speed = file_size / upload_time if upload_time > 0 else 0
        overall_speed = file_size / total_time if total_time > 0 else 0

        # Send success message
        await status_msg.delete()
        
        response_text = (
            f"**✅ ULTRA FAST UPLOAD COMPLETE!** 🚀\n\n"
            f"📁 **File:** `{original_filename}`\n"
            f"💾 **Size:** `{human_readable_size(file_size)}`\n\n"
            f"⏱️ **Download Time:** {download_time:.1f}s\n"
            f"📥 **Download Speed:** {human_readable_size(download_speed)}/s\n\n"
            f"⏱️ **Upload Time:** {upload_time:.1f}s\n"
            f"📤 **Upload Speed:** {human_readable_size(upload_speed)}/s\n\n"
            f"⏱️ **Total Time:** {total_time:.1f}s\n"
            f"🚀 **Overall Speed:** {human_readable_size(overall_speed)}/s\n\n"
            f"🔗 **Direct Stream Link:**\n`{web_link}`"
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
        logger.error(f"Ultra fast upload failed: {e}")
        await status_msg.edit_text(f"❌ Upload failed: {str(e)}")

# ==========================================
# DIRECT MEMORY UPLOAD FOR SMALL FILES
# ==========================================
@app.on_message(filters.command("turbo"))
async def turbo_upload_handler(client, message: Message):
    """TURBO MODE - Direct memory upload for maximum speed"""
    if not message.reply_to_message or not (message.reply_to_message.document or message.reply_to_message.video):
        await message.reply_text("❌ Please reply to a file with /turbo")
        return

    msg = message.reply_to_message
    media = getattr(msg, msg.media.value)
    original_filename = getattr(media, "file_name", f"file_{msg.id}")
    file_size = media.file_size

    # Limit for memory safety
    if file_size > 200 * 1024 * 1024:  # 200MB limit for turbo mode
        await message.reply_text("❌ For files over 200MB, use regular upload")
        return

    status_msg = await message.reply_text("🚀 TURBO MODE ACTIVATED - MAXIMUM SPEED!")
    start_time = time.time()

    try:
        # Download directly to memory - MAXIMUM SPEED
        file_bytes = await msg.download(in_memory=True)
        
        # Generate S3 key
        user_id = msg.from_user.id if msg.from_user else "unknown"
        s3_key = generate_s3_key(original_filename, user_id)
        
        # Upload directly from memory - ULTRA FAST
        loop = asyncio.get_running_loop()
        
        upload_start = time.time()
        success = await loop.run_in_executor(
            executor,
            lambda: s3_client.upload_fileobj(
                file_bytes, 
                s3_key
            )
        )
        upload_time = time.time() - upload_start
        
        if success:
            web_link = s3_client.generate_presigned_url(s3_key)
            total_time = time.time() - start_time
            speed = file_size / total_time if total_time > 0 else 0
            
            await status_msg.delete()
            await message.reply_text(
                f"✅ TURBO UPLOAD COMPLETE! 🚀\n\n"
                f"📁 **File:** `{original_filename}`\n"
                f"💾 **Size:** `{human_readable_size(file_size)}`\n"
                f"⏱️ **Total Time:** {total_time:.1f}s\n"
                f"🚀 **Speed:** {human_readable_size(speed)}/s\n\n"
                f"🔗 **Link:** {web_link}"
            )
        else:
            await status_msg.edit_text("❌ Turbo upload failed")
            
    except Exception as e:
        await status_msg.edit_text(f"❌ Turbo error: {str(e)}")

@app.on_message(filters.command("speed"))
async def speed_info(client, message):
    await message.reply_text(
        "**🚀 ULTRA FAST OPTIMIZATIONS**\n\n"
        "• **50MB Chunk Size** - Maximum throughput\n"
        "• **20 Concurrent Connections** - Full bandwidth\n"
        "• **No Progress Tracking** - Removed bottlenecks\n"
        "• **Aggressive Timeouts** - Faster failover\n"
        "• **Multi-threaded Uploads** - Parallel processing\n"
        "• **Memory Optimization** - Efficient resource usage\n\n"
        "**Result:** Maximum possible upload speed! ⚡"
    )

# ==========================================
# MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    logger.info("🚀 Starting ULTRA FAST Wasabi Bot...")
    print("🚀 ULTRA FAST Wasabi Upload Bot")
    print("⚡ Optimized for Maximum Speed")
    print("📊 50MB chunks, 20 concurrent connections")
    print("🎯 No progress tracking bottlenecks")
    app.run()
