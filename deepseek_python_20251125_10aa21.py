import logging
import sqlite3
import time
import asyncio
import re
from collections import defaultdict
from typing import Optional, Tuple

import aiohttp
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ChatPermissions,
    User,
    ChatMember,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
    ChatMemberHandler,
)
from telegram.constants import ParseMode, ChatMemberStatus

# ==============================================================================
# CONFIGURATION - Import from config.py
# ==============================================================================
from config import TOKEN, DB_FILE, FLOOD_LIMIT, FLOOD_WINDOW

# Logging setup
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==============================================================================
# DATABASE MANAGER
# ==============================================================================
class Database:
    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.init_db()

    def init_db(self):
        # Settings table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS chat_settings (
                chat_id INTEGER PRIMARY KEY,
                lang TEXT DEFAULT 'en',
                rules TEXT DEFAULT '',
                welcome_msg TEXT DEFAULT 'Welcome {mention} to {chat}!',
                goodbye_msg TEXT DEFAULT 'Goodbye {name}!',
                captcha_enabled INTEGER DEFAULT 0,
                anti_flood_enabled INTEGER DEFAULT 0,
                block_links INTEGER DEFAULT 0,
                block_media INTEGER DEFAULT 0,
                max_warns INTEGER DEFAULT 3
            )
        """)
        # Warns table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS warns (
                chat_id INTEGER,
                user_id INTEGER,
                count INTEGER DEFAULT 0,
                PRIMARY KEY (chat_id, user_id)
            )
        """)
        # User Privacy / Data
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_data (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_seen INTEGER
            )
        """)
        self.conn.commit()

    def get_settings(self, chat_id):
        self.cursor.execute("SELECT * FROM chat_settings WHERE chat_id=?", (chat_id,))
        res = self.cursor.fetchone()
        if not res:
            self.cursor.execute("INSERT INTO chat_settings (chat_id) VALUES (?)", (chat_id,))
            self.conn.commit()
            return self.get_settings(chat_id)
        # Map tuple to dict for easier access
        cols = [col[0] for col in self.cursor.description]
        return dict(zip(cols, res))

    def update_setting(self, chat_id, key, value):
        query = f"UPDATE chat_settings SET {key}=? WHERE chat_id=?"
        self.cursor.execute(query, (value, chat_id))
        self.conn.commit()

    def add_warn(self, chat_id, user_id):
        self.cursor.execute("SELECT count FROM warns WHERE chat_id=? AND user_id=?", (chat_id, user_id))
        res = self.cursor.fetchone()
        new_count = 1 if not res else res[0] + 1
        self.cursor.execute("""
            INSERT INTO warns (chat_id, user_id, count) VALUES (?, ?, ?)
            ON CONFLICT(chat_id, user_id) DO UPDATE SET count=excluded.count
        """, (chat_id, user_id, new_count))
        self.conn.commit()
        return new_count

    def reset_warns(self, chat_id, user_id):
        self.cursor.execute("DELETE FROM warns WHERE chat_id=? AND user_id=?", (chat_id, user_id))
        self.conn.commit()

    def forget_user(self, chat_id, user_id):
        # Removes user data from local scope
        self.cursor.execute("DELETE FROM warns WHERE chat_id=? AND user_id=?", (chat_id, user_id))
        self.cursor.execute("DELETE FROM user_data WHERE user_id=?", (user_id,))
        self.conn.commit()

db = Database(DB_FILE)

# ==============================================================================
# LOCALIZATION
# ==============================================================================
STRINGS = {
    'en': {
        'welcome': "Welcome settings updated.",
        'goodbye': "Goodbye settings updated.",
        'rules_set': "Rules have been updated.",
        'no_rules': "No rules set for this chat.",
        'muted': "User {user} has been muted.",
        'kicked': "User {user} has been kicked.",
        'banned': "User {user} has been banned.",
        'warned': "User {user} warned ({cur}/{max}).",
        'warn_ban': "User {user} banned due to max warnings.",
        'reset_warns': "Warnings reset for {user}.",
        'admin_only': "❌ This command is for admins only.",
        'captcha_button': "I am human",
        'captcha_msg': "Welcome {mention}! Please verify you are human.",
        'captcha_verified': "Verification successful. You can now chat.",
        'flood_warn': "⚠️ Stop flooding!",
        'link_del': "❌ Links are not allowed.",
        'media_del': "❌ Media is not allowed.",
        'crypto_price': "💰 {symbol}: ${price} USD",
        'crypto_fail': "Could not fetch price.",
        'data_forgotten': "✅ All data regarding this user in this group has been wiped.",
        'lang_set': "Language set to English."
    },
    'es': {
        'welcome': "Configuración de bienvenida actualizada.",
        'goodbye': "Configuración de despedida actualizada.",
        'rules_set': "Las reglas han sido actualizadas.",
        'no_rules': "No hay reglas definidas para este chat.",
        'muted': "Usuario {user} silenciado.",
        'kicked': "Usuario {user} expulsado.",
        'banned': "Usuario {user} baneado.",
        'warned': "Usuario {user} advertido ({cur}/{max}).",
        'warn_ban': "Usuario {user} baneado por exceso de advertencias.",
        'reset_warns': "Advertencias reiniciadas para {user}.",
        'admin_only': "❌ Este comando es solo para administradores.",
        'captcha_button': "Soy humano",
        'captcha_msg': "¡Bienvenido {mention}! Por favor verifica que eres humano.",
        'captcha_verified': "Verificación exitosa. Puedes chatear.",
        'flood_warn': "⚠️ ¡Deja de hacer spam!",
        'link_del': "❌ Enlaces no permitidos.",
        'media_del': "❌ Multimedia no permitida.",
        'crypto_price': "💰 {symbol}: ${price} USD",
        'crypto_fail': "No se pudo obtener el precio.",
        'data_forgotten': "✅ Datos del usuario borrados.",
        'lang_set': "Idioma cambiado a Español."
    }
}

def get_text(chat_id, key, **kwargs):
    settings = db.get_settings(chat_id)
    lang = settings.get('lang', 'en')
    text = STRINGS.get(lang, STRINGS['en']).get(key, key)
    return text.format(**kwargs)

# ==============================================================================
# HELPERS & DECORATORS
# ==============================================================================
async def check_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user
    chat = update.effective_chat
    if chat.type == "private":
        return True
    member = await chat.get_member(user.id)
    if member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
        return True
    msg = get_text(chat.id, 'admin_only')
    await update.message.reply_text(msg)
    return False

# In-memory flood control storage
# Structure: {chat_id: {user_id: [timestamp1, timestamp2, ...]}}
flood_cache = defaultdict(lambda: defaultdict(list))

# ==============================================================================
# MODERATION COMMANDS
# ==============================================================================
async def ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    if not update.message.reply_to_message:
        await update.message.reply_text("Reply to a user to ban them.")
        return
    
    user = update.message.reply_to_message.from_user
    chat = update.effective_chat
    try:
        await chat.ban_member(user.id)
        await update.message.reply_text(get_text(chat.id, 'banned', user=user.first_name))
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")

async def mute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    if not update.message.reply_to_message: return
    
    user = update.message.reply_to_message.from_user
    chat = update.effective_chat
    permissions = ChatPermissions(can_send_messages=False)
    
    try:
        await chat.restrict_member(user.id, permissions=permissions)
        await update.message.reply_text(get_text(chat.id, 'muted', user=user.first_name))
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")

async def kick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    if not update.message.reply_to_message: return
    
    user = update.message.reply_to_message.from_user
    chat = update.effective_chat
    try:
        await chat.unban_member(user.id) # Unban immediately effectively kicks
        await update.message.reply_text(get_text(chat.id, 'kicked', user=user.first_name))
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")

async def warn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    if not update.message.reply_to_message: return

    user = update.message.reply_to_message.from_user
    chat_id = update.effective_chat.id
    settings = db.get_settings(chat_id)
    max_warns = settings['max_warns']

    cur_warns = db.add_warn(chat_id, user.id)

    if cur_warns >= max_warns:
        await update.effective_chat.ban_member(user.id)
        db.reset_warns(chat_id, user.id)
        await update.message.reply_text(get_text(chat_id, 'warn_ban', user=user.first_name))
    else:
        await update.message.reply_text(get_text(chat_id, 'warned', user=user.first_name, cur=cur_warns, max=max_warns))

async def unwarn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    if not update.message.reply_to_message: return

    user = update.message.reply_to_message.from_user
    db.reset_warns(update.effective_chat.id, user.id)
    await update.message.reply_text(get_text(update.effective_chat.id, 'reset_warns', user=user.first_name))

async def forget(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove user-data from group context"""
    if not await check_admin(update, context): return
    
    target_id = update.effective_user.id
    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
    
    db.forget_user(update.effective_chat.id, target_id)
    await update.message.reply_text(get_text(update.effective_chat.id, 'data_forgotten'))

# ==============================================================================
# SETTINGS COMMANDS
# ==============================================================================
async def set_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    chat_id = update.effective_chat.id
    
    if context.args and context.args[0] in STRINGS:
        lang_code = context.args[0]
        db.update_setting(chat_id, 'lang', lang_code)
        await update.message.reply_text(STRINGS[lang_code]['lang_set'])
    else:
        await update.message.reply_text(f"Available languages: {', '.join(STRINGS.keys())}")

async def set_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    chat_id = update.effective_chat.id
    
    # Check if command has text after it or if it's a reply
    text_to_save = ""
    if context.args:
        text_to_save = ' '.join(context.args)
    elif update.message.reply_to_message and update.message.reply_to_message.text:
        text_to_save = update.message.reply_to_message.text
        
    if text_to_save:
        db.update_setting(chat_id, 'rules', text_to_save)
        await update.message.reply_text(get_text(chat_id, 'rules_set'))
    else:
        await update.message.reply_text("Usage: /setrules <text> OR reply to a message with /setrules")

async def get_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    settings = db.get_settings(chat_id)
    rules = settings.get('rules')
    if rules:
        await update.message.reply_text(rules)
    else:
        await update.message.reply_text(get_text(chat_id, 'no_rules'))

async def toggle_setting(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin(update, context): return
    
    # Usage: /toggle captcha, /toggle antiflood, /toggle links, /toggle media
    if not context.args:
        await update.message.reply_text("Usage: /toggle <captcha|antiflood|links|media>")
        return

    setting = context.args[0].lower()
    chat_id = update.effective_chat.id
    current = db.get_settings(chat_id)
    
    map_key = {
        'captcha': 'captcha_enabled',
        'antiflood': 'anti_flood_enabled',
        'links': 'block_links',
        'media': 'block_media'
    }
    
    if setting in map_key:
        key = map_key[setting]
        new_val = 0 if current[key] else 1
        db.update_setting(chat_id, key, new_val)
        state = "Enabled" if new_val else "Disabled"
        await update.message.reply_text(f"{setting.capitalize()} is now {state}.")
    else:
        await update.message.reply_text("Unknown setting.")

# ==============================================================================
# CRYPTO API
# ==============================================================================
async def crypto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Usage: /crypto btc"""
    chat_id = update.effective_chat.id
    symbol = context.args[0].upper() if context.args else "BTC"
    
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}USDT"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            data = await resp.json()
            if 'price' in data:
                price = float(data['price'])
                await update.message.reply_text(
                    get_text(chat_id, 'crypto_price', symbol=symbol, price=f"{price:,.2f}")
                )
            else:
                await update.message.reply_text(get_text(chat_id, 'crypto_fail'))

# ==============================================================================
# EVENT HANDLERS
# ==============================================================================

async def handle_new_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Welcome + Captcha Logic"""
    chat_id = update.effective_chat.id
    settings = db.get_settings(chat_id)
    
    for user in update.message.new_chat_members:
        if user.is_bot: continue

        # Captcha Logic
        if settings['captcha_enabled']:
            # Mute user first
            await update.effective_chat.restrict_member(
                user.id,
                permissions=ChatPermissions(can_send_messages=False)
            )
            
            # Send Captcha Button
            keyboard = [[InlineKeyboardButton(
                get_text(chat_id, 'captcha_button'), 
                callback_data=f"captcha_{user.id}"
            )]]
            
            msg_text = get_text(chat_id, 'captcha_msg', mention=user.mention_html())
            await update.message.reply_html(
                msg_text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            # Regular Welcome
            welcome_template = settings['welcome_msg']
            await update.message.reply_text(
                welcome_template.format(mention=user.full_name, chat=update.effective_chat.title)
            )

async def handle_left_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.left_chat_member
    if user.is_bot: return
    chat_id = update.effective_chat.id
    settings = db.get_settings(chat_id)
    
    msg = settings['goodbye_msg'].format(name=user.full_name)
    await update.message.reply_text(msg)

async def captcha_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    user_id = query.from_user.id
    
    if data.startswith("captcha_"):
        target_id = int(data.split("_")[1])
        if user_id == target_id:
            chat_id = update.effective_chat.id
            # Unmute
            permissions = ChatPermissions(
                can_send_messages=True,
                can_send_media_messages=True,
                can_send_other_messages=True
            )
            await update.effective_chat.restrict_member(user_id, permissions=permissions)
            
            await query.answer(get_text(chat_id, 'captcha_verified'))
            await query.message.delete()
        else:
            await query.answer("This button is not for you.", show_alert=True)

async def message_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles Anti-flood, Anti-spam (links), Media Blocks"""
    if not update.message or not update.message.from_user: return
    
    user = update.message.from_user
    chat_id = update.effective_chat.id
    settings = db.get_settings(chat_id)
    
    # 1. Anti-Flood Check
    if settings['anti_flood_enabled']:
        now = time.time()
        user_flood = flood_cache[chat_id][user.id]
        # Keep only recent timestamps
        user_flood[:] = [t for t in user_flood if now - t < FLOOD_WINDOW]
        user_flood.append(now)
        
        if len(user_flood) > FLOOD_LIMIT:
            await update.message.reply_text(get_text(chat_id, 'flood_warn'))
            # Optional: Mute user temporarily
            return

    # 2. Block Links
    if settings['block_links']:
        # Basic regex for urls
        if re.search(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', update.message.text or ""):
            # Check if admin
            mem = await update.effective_chat.get_member(user.id)
            if mem.status not in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                await update.message.delete()
                await update.message.reply_text(get_text(chat_id, 'link_del'))
                return

    # 3. Block Media
    if settings['block_media']:
        if update.message.photo or update.message.video or update.message.document or update.message.sticker:
             # Check if admin
            mem = await update.effective_chat.get_member(user.id)
            if mem.status not in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                await update.message.delete()
                await update.message.reply_text(get_text(chat_id, 'media_del'))
                return

# ==============================================================================
# MAIN APPLICATION
# ==============================================================================
def main():
    if TOKEN == "YOUR_ACTUAL_BOT_TOKEN_HERE":
        print("Error: Please set your bot TOKEN in the config.py file.")
        return

    # Create App
    application = Application.builder().token(TOKEN).build()

    # Commands
    application.add_handler(CommandHandler("ban", ban))
    application.add_handler(CommandHandler("mute", mute))
    application.add_handler(CommandHandler("kick", kick))
    application.add_handler(CommandHandler("warn", warn))
    application.add_handler(CommandHandler("unwarn", unwarn))
    application.add_handler(CommandHandler("forget", forget))
    
    # Settings Commands
    application.add_handler(CommandHandler("lang", set_lang))
    application.add_handler(CommandHandler("setrules", set_rules))
    application.add_handler(CommandHandler("rules", get_rules))
    application.add_handler(CommandHandler("toggle", toggle_setting))
    
    # Utils
    application.add_handler(CommandHandler("crypto", crypto))

    # Event Handlers
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, handle_new_members))
    application.add_handler(MessageHandler(filters.StatusUpdate.LEFT_CHAT_MEMBER, handle_left_member))
    application.add_handler(CallbackQueryHandler(captcha_callback))
    
    # Message Filter (Must be last to capture text)
    application.add_handler(MessageHandler(filters.TEXT | filters.ATTACHMENT, message_filter))

    print("Bot is running...")
    application.run_polling()

if __name__ == "__main__":
    main()