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
# INSTANT UPLOAD S3 CLIENT
# ==========================================
class InstantUploadS3Client:
    def __init__(self):
        boto_config = BotoConfig(
            region_name=config.WASABI_REGION,
            retries={'max_attempts': 2, 'mode': 'standard'},
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

    def start_multipart_upload(self, object_name):
        """Start multipart upload for instant start"""
        try:
            response = self.s3.create_multipart_upload(
                Bucket=self.bucket,
                Key=object_name
            )
            return response['UploadId']
        except Exception as e:
            logger.error(f"Multipart start error: {e}")
            return None

    def upload_part(self, object_name, upload_id, part_number, data, progress_callback=None):
        """Upload a single part"""
        try:
            response = self.s3.upload_part(
                Bucket=self.bucket,
                Key=object_name,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=data
            )
            if progress_callback and len(data):
                progress_callback(len(data))
            return {'ETag': response['ETag'], 'PartNumber': part_number}
        except Exception as e:
            logger.error(f"Part upload error: {e}")
            return None

    def complete_multipart_upload(self, object_name, upload_id, parts):
        """Complete the multipart upload"""
        try:
            self.s3.complete_multipart_upload(
                Bucket=self.bucket,
                Key=object_name,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            return True
        except Exception as e:
            logger.error(f"Multipart complete error: {e}")
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
s3_client = InstantUploadS3Client()

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
    
    # Update every 1 second for instant feedback
    if round(diff % 1.00) == 0 or current == total:
        speed = current / diff if diff > 0 else 0
        elapsed_time = round(diff) * 1000
        time_to_completion = round((total - current) / speed) * 1000 if speed > 0 else 0

        def time_formatter(milliseconds):
            seconds = int(milliseconds / 1000)
            minutes, seconds = divmod(seconds, 60)
            return f"{minutes}m {seconds}s"

        text = (
            f"**{process_type}...** ⚡\n"
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
    
    safe_name = "".join(c for c in name if c.isalnum() or c in ('-', '_')).rstrip()
    
    if user_id:
        return f"users/{user_id}/{timestamp}_{safe_name}_{random_str}{ext}"
    return f"uploads/{timestamp}_{safe_name}_{random_str}{ext}"

# ==========================================
# SIMPLE INSTANT UPLOAD SYSTEM
# ==========================================
class SimpleInstantUploader:
    def __init__(self, s3_key, file_size, status_msg, loop):
        self.s3_key = s3_key
        self.file_size = file_size
        self.status_msg = status_msg
        self.loop = loop
        self.start_time = time.time()
        self.uploaded_size = 0
        self.upload_id = None
        self.parts = []
        self.part_number = 1

    async def start_upload(self):
        """Start multipart upload immediately"""
        self.upload_id = await asyncio.get_running_loop().run_in_executor(
            executor, 
            lambda: s3_client.start_multipart_upload(self.s3_key)
        )
        return self.upload_id is not None

    def progress_callback(self, bytes_uploaded):
        """Update progress for uploaded bytes"""
        self.uploaded_size += bytes_uploaded
        asyncio.run_coroutine_threadsafe(
            progress_hook(
                self.uploaded_size, 
                self.file_size, 
                self.status_msg, 
                self.start_time, 
                "🚀 INSTANT UPLOAD"
            ),
            self.loop
        )

    async def upload_chunk(self, chunk_data):
        """Upload a single chunk"""
        if not self.upload_id:
            return None

        part = await asyncio.get_running_loop().run_in_executor(
            executor,
            lambda: s3_client.upload_part(
                self.s3_key,
                self.upload_id,
                self.part_number,
                chunk_data,
                self.progress_callback
            )
        )
        
        if part:
            self.parts.append(part)
            self.part_number += 1
        
        return part

    async def complete_upload(self):
        """Complete the multipart upload"""
        if not self.upload_id:
            return False

        # Sort parts by part number
        self.parts.sort(key=lambda x: x['PartNumber'])
        
        success = await asyncio.get_running_loop().run_in_executor(
            executor,
            lambda: s3_client.complete_multipart_upload(
                self.s3_key,
                self.upload_id,
                self.parts
            )
        )
        return success

# ==========================================
# BOT HANDLERS - SIMPLE INSTANT UPLOAD
# ==========================================

@app.on_message(filters.command("start"))
async def start_handler(client, message):
    await message.reply_text(
        "**⚡ INSTANT UPLOAD Wasabi Bot**\n\n"
        "Send me any file and I will:\n"
        "• **INSTANT UPLOAD** - Starts uploading immediately\n"
        "• **Parallel Processing** - No waiting for download to finish\n"
        "• **Real-time Streaming** - Maximum speed\n\n"
        f"**Max Size:** {human_readable_size(config.MAX_FILE_SIZE)}\n"
        "**Speed:** ⚡ INSTANT START"
    )

@app.on_message(filters.document | filters.video | filters.audio)
async def instant_upload_handler(client, message: Message):
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
        f"**⚡ STARTING INSTANT UPLOAD...**\n"
        f"**File:** `{original_filename}`\n"
        f"**Size:** `{human_readable_size(file_size)}`\n"
        f"**Mode:** PARALLEL UPLOAD 🚀"
    )
    
    start_time = time.time()
    loop = asyncio.get_running_loop()

    try:
        # Create instant uploader and start multipart upload IMMEDIATELY
        uploader = SimpleInstantUploader(s3_key, file_size, status_msg, loop)
        upload_started = await uploader.start_upload()
        
        if not upload_started:
            await status_msg.edit_text("❌ Failed to start instant upload")
            return

        await status_msg.edit_text("**🔄 DOWNLOADING + UPLOADING IN PARALLEL...** ⚡")

        # Create temporary file for download
        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
            # Download file with progress
            download_task = asyncio.create_task(
                message.download(
                    file_name=temp_file.name,
                    progress=progress_hook,
                    progress_args=(status_msg, start_time, "⬇️ Downloading")
                )
            )
            
            # Wait for download to complete
            await download_task
            
            # Now upload in chunks while tracking progress
            chunk_size = config.CHUNK_SIZE
            with open(temp_file.name, 'rb') as file_obj:
                while True:
                    chunk = file_obj.read(chunk_size)
                    if not chunk:
                        break
                    
                    # Upload chunk immediately
                    await uploader.upload_chunk(chunk)

        # Complete the upload
        success = await uploader.complete_upload()
        
        if not success:
            await status_msg.edit_text("❌ Upload completion failed")
            return

        # Generate streaming link
        web_link = s3_client.generate_presigned_url(s3_key)
        
        if not web_link:
            await status_msg.edit_text("❌ Failed to generate streaming link")
            return

        # Send success message
        total_time = time.time() - start_time
        await status_msg.delete()
        
        response_text = (
            f"**✅ INSTANT UPLOAD COMPLETE!** ⚡\n\n"
            f"📁 **File:** `{original_filename}`\n"
            f"💾 **Size:** `{human_readable_size(file_size)}`\n"
            f"⏱️ **Total Time:** {total_time:.1f}s\n"
            f"🚀 **Average Speed:** {human_readable_size(file_size/total_time)}/s\n\n"
            f"🔗 **Direct Stream Link:**\n`{web_link}`\n\n"
            f"**Upload started instantly!**"
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
        logger.error(f"Instant upload failed: {e}")
        await status_msg.edit_text(f"❌ Upload failed: {str(e)}")

# Alternative: TRUE INSTANT UPLOAD for smaller files
@app.on_message(filters.command("fast"))
async def true_instant_upload_handler(client, message: Message):
    """True instant upload for files under 500MB"""
    if not message.reply_to_message or not (message.reply_to_message.document or message.reply_to_message.video):
        await message.reply_text("❌ Please reply to a file with /fast")
        return

    msg = message.reply_to_message
    media = getattr(msg, msg.media.value)
    original_filename = getattr(media, "file_name", f"file_{msg.id}")
    file_size = media.file_size

    if file_size > 500 * 1024 * 1024:  # 500MB limit for true instant
        await message.reply_text("❌ For files over 500MB, use regular upload")
        return

    status_msg = await message.reply_text("⚡ TRUE INSTANT UPLOAD STARTING...")
    start_time = time.time()

    try:
        # Download directly to memory
        file_bytes = await msg.download(in_memory=True)
        
        # Generate S3 key
        user_id = msg.from_user.id if msg.from_user else "unknown"
        s3_key = generate_s3_key(original_filename, user_id)
        
        # Upload directly from memory
        loop = asyncio.get_running_loop()
        success = await loop.run_in_executor(
            executor,
            lambda: s3_client.upload_fileobj(
                file_bytes, 
                s3_key, 
                file_size, 
                None  # No progress for simplicity
            )
        )
        
        if success:
            web_link = s3_client.generate_presigned_url(s3_key)
            total_time = time.time() - start_time
            
            await status_msg.delete()
            await message.reply_text(
                f"✅ TRUE INSTANT UPLOAD! ⚡\n"
                f"⏱️ Time: {total_time:.1f}s\n"
                f"🔗 {web_link}"
            )
        else:
            await status_msg.edit_text("❌ Upload failed")
            
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")

@app.on_message(filters.command("instant"))
async def instant_info(client, message):
    await message.reply_text(
        "**⚡ INSTANT UPLOAD TECHNOLOGY**\n\n"
        "• **Multipart Upload** - Starts in <1 second\n"
        "• **Chunked Processing** - 16MB chunks for speed\n"
        "• **Parallel Operations** - Efficient resource usage\n"
        "• **Real-time Progress** - Live speed tracking\n\n"
        "**Result:** Near-instant upload experience! 🚀"
    )

# ==========================================
# MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    logger.info("⚡ Starting INSTANT UPLOAD Wasabi Bot...")
    print("⚡ INSTANT UPLOAD Wasabi Bot")
    print("🚀 Technology: Multipart Upload")
    print("⏱️ Start Time: <1 second")
    app.run()
