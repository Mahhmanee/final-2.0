# -*- coding: utf-8 -*-
import asyncio
import datetime as dt
import os
from typing import Dict, Optional, List

import aiosqlite
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, User as TgUser
)
from telegram.constants import ChatType
from telegram.ext import (
    ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters
)

# ========= НАСТРОЙКИ =========
BOT_TOKEN = os.getenv("BOT_TOKEN", "8351785031:AAEa4AgLciZGVO0cHm_Aa4SLqBINzbDDjao")   # Заполните в Railway Variables
MOD_GROUP_ID = int(os.getenv("MOD_GROUP_ID", "-1003173446264"))  # Заполните в Railway Variables
DB_PATH = os.getenv("DB_PATH", "support.db")

LANGS = {"ru": "Русский", "en": "English"}
CATS = {
    "ru": [
        ("🔧 Техническая помощь", "tech"),
        ("💳 Помощь с платежами", "pay"),
        ("🔄 Сброс HWID", "hwid"),
        ("🤝 Сотрудничество", "coop"),
        ("❓ FAQ / Цены / Товары", "faq")
    ],
    "en": [
        ("🔧 Technical Support", "tech"),
        ("💳 Payment Help", "pay"),
        ("🔄 HWID Reset", "hwid"),
        ("🤝 Cooperation", "coop"),
        ("❓ FAQ / Prices / Products", "faq")
    ]
}
CAT_TITLES_RU = {
    "tech": "🔧 Техническая помощь",
    "pay": "💳 Помощь с платежами",
    "hwid": "🔄 Сброс HWID",
    "coop": "🤝 Сотрудничество",
    "faq": "❓ FAQ / Цены / Товары",
}

# Активные «сессии ответа» модераторов: mod_id -> ticket_id
active_reply: Dict[int, str] = {}

# ========= БАЗА ДАННЫХ =========
INIT_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  lang TEXT DEFAULT 'ru'
);

CREATE TABLE IF NOT EXISTS tickets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ticket_id TEXT UNIQUE,
  user_id INTEGER NOT NULL,
  category TEXT NOT NULL,
  reason TEXT,
  description TEXT,
  status TEXT NOT NULL DEFAULT 'open',           -- open/closed
  created_at TEXT NOT NULL,
  assigned_to INTEGER,                           -- id модератора, взявшего тикет
  closed_by INTEGER,                             -- id модератора, кто закрыл
  closed_by_name TEXT,                           -- username/имя модератора при закрытии
  group_header_msg_id INTEGER                    -- id главного сообщения-«карточки» в группе
);

CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ticket_id TEXT NOT NULL,
  from_role TEXT NOT NULL,                       -- 'user' | 'mod' | 'system'
  text TEXT,
  user_msg_id INTEGER,                           -- message_id у пользователя
  group_msg_id INTEGER,                          -- message_id в группе модерации (для удаления)
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT
);

-- Автоответчики по категориям
CREATE TABLE IF NOT EXISTS autoresponders (
  category TEXT PRIMARY KEY,                     -- tech|pay|hwid|coop|faq
  text TEXT
);
"""

# ✅ стабильное подключение к SQLite для Python 3.13.x
async def adb():
    conn = await aiosqlite.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = aiosqlite.Row
    return conn

async def init_db():
    async with await adb() as conn:
        await conn.executescript(INIT_SQL)
        # Автоответчики включены по умолчанию
        await conn.execute(
            "INSERT INTO settings(key,value) VALUES('autoresponders_enabled','1') "
            "ON CONFLICT(key) DO NOTHING"
        )
        await conn.commit()
    print("✅ Database initialized.")

def gen_ticket_id(seq: int) -> str:
    today = dt.datetime.now().strftime("%Y%m%d")
    return f"T-{today}-{seq:04d}"

# ========= ХЕЛПЕРЫ ДЛЯ БД =========
async def set_user_lang(uid: int, lang: str):
    async with await adb() as conn:
        await conn.execute(
            "INSERT INTO users(user_id,lang) VALUES(?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET lang=excluded.lang",
            (uid, lang))
        await conn.commit()

async def get_user_lang(uid: int) -> str:
    async with await adb() as conn:
        cur = await conn.execute("SELECT lang FROM users WHERE user_id=?", (uid,))
        row = await cur.fetchone()
        return row["lang"] if row else "ru"

async def autores_enabled() -> bool:
    async with await adb() as conn:
        cur = await conn.execute("SELECT value FROM settings WHERE key='autoresponders_enabled'")
        row = await cur.fetchone()
        return (row and row["value"] == "1")

async def set_autores_enabled(enabled: bool):
    async with await adb() as conn:
        await conn.execute(
            "INSERT INTO settings(key,value) VALUES('autoresponders_enabled',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("1" if enabled else "0",))
        await conn.commit()

async def get_autoresponder_text(category: str) -> Optional[str]:
    async with await adb() as conn:
        cur = await conn.execute("SELECT text FROM autoresponders WHERE category=?", (category,))
        row = await cur.fetchone()
        return row["text"] if row else None

async def set_autoresponder_text(category: str, text: str):
    async with await adb() as conn:
        await conn.execute(
            "INSERT INTO autoresponders(category,text) VALUES(?,?) "
            "ON CONFLICT(category) DO UPDATE SET text=excluded.text",
            (category, text))
        await conn.commit()

async def create_ticket(user_id: int, category: str, reason: str, description: str) -> str:
    now = dt.datetime.utcnow().isoformat()
    async with await adb() as conn:
        # создаём запись, получаем id, формируем ticket_id
        await conn.execute(
            "INSERT INTO tickets(ticket_id,user_id,category,reason,description,status,created_at) "
            "VALUES(?,?,?,?,?,'open',?)", ("", user_id, category, reason, description, now))
        await conn.commit()
        cur = await conn.execute("SELECT id FROM tickets WHERE user_id=? ORDER BY id DESC LIMIT 1", (user_id,))
        row = await cur.fetchone()
        seq = row["id"]
        t_id = gen_ticket_id(seq)
        await conn.execute("UPDATE tickets SET ticket_id=? WHERE id=?", (t_id, seq))
        await conn.commit()
        return t_id

async def store_group_header(ticket_id: str, msg_id: int):
    async with await adb() as conn:
        await conn.execute("UPDATE tickets SET group_header_msg_id=? WHERE ticket_id=?", (msg_id, ticket_id))
        await conn.commit()

async def mark_assigned(ticket_id: str, mod_id: int):
    async with await adb() as conn:
        await conn.execute("UPDATE tickets SET assigned_to=? WHERE ticket_id=?", (mod_id, ticket_id))
        await conn.commit()

async def get_ticket_user(ticket_id: str) -> Optional[int]:
    async with await adb() as conn:
        cur = await conn.execute("SELECT user_id FROM tickets WHERE ticket_id=?", (ticket_id,))
        r = await cur.fetchone()
        return r["user_id"] if r else None

async def get_ticket_header(ticket_id: str) -> Optional[int]:
    async with await adb() as conn:
        cur = await conn.execute("SELECT group_header_msg_id FROM tickets WHERE ticket_id=?", (ticket_id,))
        r = await cur.fetchone()
        return r["group_header_msg_id"] if r else None

async def record_msg(ticket_id: str, role: str, text: str, user_msg_id: int | None, group_msg_id: int | None):
    async with await adb() as conn:
        await conn.execute(
            "INSERT INTO messages(ticket_id,from_role,text,user_msg_id,group_msg_id,created_at) "
            "VALUES(?,?,?,?,?,?)",
            (ticket_id, role, text or "", user_msg_id, group_msg_id, dt.datetime.utcnow().isoformat()))
        await conn.commit()

async def get_ticket_group_msg_ids(ticket_id: str) -> List[int]:
    async with await adb() as conn:
        cur = await conn.execute("SELECT group_msg_id FROM messages WHERE ticket_id=? AND group_msg_id IS NOT NULL",
                                 (ticket_id,))
        rows = await cur.fetchall()
        return [r["group_msg_id"] for r in rows if r["group_msg_id"]]

async def ticket_exists(ticket_id: str) -> bool:
    async with await adb() as conn:
        cur = await conn.execute("SELECT 1 FROM tickets WHERE ticket_id=?", (ticket_id,))
        return (await cur.fetchone()) is not None

async def ticket_status(ticket_id: str) -> Optional[str]:
    async with await adb() as conn:
        cur = await conn.execute("SELECT status FROM tickets WHERE ticket_id=?", (ticket_id,))
        r = await cur.fetchone()
        return r["status"] if r else None

async def close_ticket(ticket_id: str, closed_by: Optional[int], closed_by_name: Optional[str]):
    async with await adb() as conn:
        await conn.execute(
            "UPDATE tickets SET status='closed', closed_by=?, closed_by_name=? WHERE ticket_id=?",
            (closed_by, closed_by_name, ticket_id)
        )
        await conn.commit()

async def ticket_history_text(ticket_id: str, limit: int = 30) -> str:
    async with await adb() as conn:
        cur = await conn.execute(
            "SELECT from_role, text, created_at FROM messages WHERE ticket_id=? ORDER BY id ASC",
            (ticket_id,))
        rows = await cur.fetchall()
    if not rows:
        return f"📜 История по {ticket_id}: сообщений нет."
    rows = rows[-limit:]
    parts = [f"📜 История по {ticket_id} (последние {len(rows)}):", ""]
    for r in rows:
        role = {"user": "👤 Пользователь", "mod": "🛠 Модератор", "system": "📎 Система"}.get(r["from_role"], r["from_role"])
        txt = (r["text"] or "").strip()
        if len(txt) > 600:
            txt = txt[:600] + "…"
        parts.append(f"{role}:\n{txt}\n")
    return "\n".join(parts)

async def stats_text() -> str:
    async with await adb() as conn:
        cur = await conn.execute("""
            SELECT COALESCE(closed_by_name, CAST(closed_by AS TEXT)) as who, COUNT(*) c
            FROM tickets
            WHERE status='closed' AND closed_by IS NOT NULL
            GROUP BY who
            ORDER BY c DESC
        """)
        rows = await cur.fetchall()
    if not rows:
        return "📊 Пока никто не закрыл ни одного тикета."
    out = ["📊 Статистика закрытий:"]
    for r in rows:
        out.append(f"- {r['who']}: {r['c']}")
    return "\n".join(out)

async def last_tickets(limit: int = 10) -> List[str]:
    async with await adb() as conn:
        cur = await conn.execute("SELECT ticket_id FROM tickets ORDER BY id DESC LIMIT ?", (limit,))
        rows = await cur.fetchall()
    return [r["ticket_id"] for r in rows]

# ========= КНОПКИ =========
def ticket_keyboard(ticket_id: str, assigned_to: Optional[int]=None) -> InlineKeyboardMarkup:
    assigned_str = f"👨‍💻 В работе у {assigned_to}" if assigned_to else "🤷‍♂️ Свободен"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📜 История", callback_data=f"t:{ticket_id}:hist"),
         InlineKeyboardButton("✋ Взять тикет", callback_data=f"t:{ticket_id}:take")],
        [InlineKeyboardButton("✉️ Ответить", callback_data=f"t:{ticket_id}:reply"),
         InlineKeyboardButton("✅ Закрыть", callback_data=f"t:{ticket_id}:close")],
        [InlineKeyboardButton(f"{assigned_str}", callback_data=f"t:{ticket_id}:noop")]
    ])

def panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Статистика", callback_data="p:stats"),
         InlineKeyboardButton("📜 История", callback_data="p:history")],
        [InlineKeyboardButton("🤖 Автоответчики", callback_data="p:autores"),
         InlineKeyboardButton("📟 Статус бота", callback_data="p:status")]
    ])

def stats_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔁 Обновить", callback_data="p:stats:refresh")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="p:back")]
    ])

def history_menu_keyboard(ids: List[str]) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for i, tid in enumerate(ids, 1):
        row.append(InlineKeyboardButton(tid, callback_data=f"p:history:show:{tid}"))
        if i % 2 == 0:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="p:back")])
    return InlineKeyboardMarkup(rows)

def autores_menu_keyboard(enabled: bool) -> InlineKeyboardMarkup:
    toggle = "🔘 Автоответчики [ON]" if enabled else "⚪️ Автоответчики [OFF]"
    rows = [[InlineKeyboardButton(CAT_TITLES_RU[c], callback_data=f"ar:cat:{c}")]
            for c in ["tech","pay","hwid","coop","faq"]]
    rows.append([InlineKeyboardButton(toggle, callback_data="ar:toggle")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="p:back")])
    return InlineKeyboardMarkup(rows)

def autores_cat_keyboard(cat: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Изменить автоответ", callback_data=f"ar:edit:{cat}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="p:autores")]
    ])

# ========= ПОЛЬЗОВАТЕЛЬ =========
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang:ru"),
         InlineKeyboardButton("🇬🇧 English", callback_data="lang:en")]
    ])
    await update.effective_message.reply_text("Выберите язык / Choose your language:", reply_markup=kb)

async def cb_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    lang = q.data.split(":")[1]
    await set_user_lang(q.from_user.id, lang)
    cats = CATS[lang]
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(title, callback_data=f"cat:{code}")]
                               for title, code in cats])
    text = "Выберите нужную услугу:" if lang == "ru" else "Choose the service you need:"
    await q.message.reply_text(text, reply_markup=kb)

async def cb_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    lang = await get_user_lang(uid)
    cat = q.data.split(":")[1]
    context.user_data["new_ticket_cat"] = cat
    context.user_data["stage"] = "reason"
    text = "Пожалуйста, коротко укажите причину обращения:" if lang == "ru" else "Please briefly describe your reason:"
    await q.message.reply_text(text)

async def pm_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    uid = update.effective_user.id
    lang = await get_user_lang(uid)
    text = update.effective_message.text or update.effective_message.caption or ""
    stage = context.user_data.get("stage")

    # 1) причина
    if stage == "reason":
        context.user_data["reason"] = text
        context.user_data["stage"] = "description"
        t = "Опишите подробнее вашу проблему:" if lang == "ru" else "Please describe your problem in detail:"
        await update.effective_message.reply_text(t)
        return

    # 2) описание + создание тикета
    if stage == "description":
        cat = context.user_data.get("new_ticket_cat")
        reason = context.user_data.get("reason", "")
        description = text
        t_id = await create_ticket(uid, cat, reason, description)

        confirm = (f"✅ Тикет {t_id} создан.\n"
                   f"Модераторы скоро ответят здесь.\nЧтобы закрыть тикет, используйте /close") if lang == "ru" else \
                  (f"✅ Ticket {t_id} created.\nModerators will reply here soon.\nUse /close to close the ticket.")
        await update.effective_message.reply_text(confirm)

        header = (f"🆕 Новый тикет {t_id}\n"
                  f"Категория: {CAT_TITLES_RU.get(cat, cat)}\n"
                  f"Причина: {reason or '—'}\n"
                  f"Описание: {description or '—'}\n"
                  f"От: @{update.effective_user.username or update.effective_user.full_name} (ID: {uid})")
        hmsg = await context.bot.send_message(MOD_GROUP_ID, header, reply_markup=ticket_keyboard(t_id))
        await store_group_header(t_id, hmsg.message_id)
        await record_msg(t_id, "system", header, None, hmsg.message_id)

        # Автоответчик по категории
        if await autores_enabled():
            atext = await get_autoresponder_text(cat)
            if atext:
                await context.bot.send_message(chat_id=uid, text=atext)

        # Лог в историю
        await record_msg(t_id, "user", f"[Причина] {reason}\n[Описание] {description}",
                         update.effective_message.message_id, None)
        context.user_data.clear()
        return

    # 3) Доп. сообщения пользователя — ищем последний open-тикет и пересылаем в группу
    async with await adb() as conn:
        cur = await conn.execute(
            "SELECT ticket_id FROM tickets WHERE user_id=? AND status='open' ORDER BY id DESC LIMIT 1",
            (uid,))
        row = await cur.fetchone()
    if not row:
        t = "Чтобы создать тикет, нажмите /start и выберите раздел." if lang == "ru" else \
            "To create a ticket, press /start and choose a section."
        await update.effective_message.reply_text(t)
        return
    t_id = row["ticket_id"]

    # шапка
    head = f"[{t_id}] Сообщение от пользователя @{update.effective_user.username or update.effective_user.full_name} (ID: {uid}):"
    h = await context.bot.send_message(MOD_GROUP_ID, head)
    await record_msg(t_id, "system", head, None, h.message_id)

    # копируем контент (включая медиа)
    copied = await context.bot.copy_message(
        chat_id=MOD_GROUP_ID,
        from_chat_id=uid,
        message_id=update.effective_message.message_id
    )
    await record_msg(t_id, "user", text or "[media]",
                     update.effective_message.message_id, copied.message_id)

# ========= КНОПКИ ТИКЕТА В ГРУППЕ =========
async def cb_ticket_actions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data  # t:<ticket_id>:action
    try:
        _, ticket_id, action = data.split(":")
    except ValueError:
        return
    if q.message.chat.id != MOD_GROUP_ID:
        return

    mod: TgUser = q.from_user

    if action == "hist":
        txt = await ticket_history_text(ticket_id, limit=30)
        await q.message.reply_text(txt, reply_to_message_id=q.message.message_id)
        return

    if action == "take":
        await mark_assigned(ticket_id, mod.id)
        active_reply.pop(mod.id, None)
        kb = ticket_keyboard(ticket_id, assigned_to=mod.id)
        try:
            await q.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass
        await q.message.reply_text(f"Тикет {ticket_id} взят в работу @{mod.username or mod.full_name}")
        return

    if action == "reply":
        active_reply[mod.id] = ticket_id
        await q.message.reply_text(
            f"✍️ Режим ответа включён для {ticket_id}. "
            f"Все ваши сообщения в этой группе будут пересылаться пользователю, пока не введёте /end."
        )
        return

    if action == "close":
        if not await ticket_exists(ticket_id):
            await q.message.reply_text("Тикет не найден.")
            return
        if await ticket_status(ticket_id) == "closed":
            await q.message.reply_text("Тикет уже закрыт.")
            return

        # удалить все групповые сообщения тикета
        gids = await get_ticket_group_msg_ids(ticket_id)
        for mid in gids:
            try:
                await context.bot.delete_message(MOD_GROUP_ID, mid)
                await asyncio.sleep(0.03)
            except Exception:
                pass

        who_name = f"@{mod.username}" if mod.username else mod.full_name
        await close_ticket(ticket_id, mod.id, who_name)
        uid = await get_ticket_user(ticket_id)
        if uid:
            try:
                await context.bot.send_message(uid, f"Тикет {ticket_id} закрыт модератором.")
            except Exception:
                pass
        await q.message.reply_text(f"✅ Тикет {ticket_id} закрыт и сообщения удалены.")
        if active_reply.get(mod.id) == ticket_id:
            active_reply.pop(mod.id, None)
        return

    if action == "noop":
        return

# ========= ПЕРЕСЫЛКА ОТ МОДЕРАЦИИ ПОЛЬЗОВАТЕЛЮ =========
async def mod_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != MOD_GROUP_ID:
        return
    mod_id = update.effective_user.id
    ticket_id = active_reply.get(mod_id)
    if not ticket_id:
        return
    # игнор служебных команд
    if update.effective_message.text and update.effective_message.text.startswith(("/", ".")):
        return
    uid = await get_ticket_user(ticket_id)
    if not uid:
        return

    await context.bot.copy_message(
        chat_id=uid,
        from_chat_id=MOD_GROUP_ID,
        message_id=update.effective_message.message_id
    )
    text = update.effective_message.text or update.effective_message.caption or "[media]"
    await record_msg(ticket_id, "mod", text, None, update.effective_message.message_id)

async def cmd_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != MOD_GROUP_ID:
        return
    mod_id = update.effective_user.id
    if mod_id in active_reply:
        ticket_id = active_reply.pop(mod_id)
        await update.effective_message.reply_text(f"🛑 Режим ответа для {ticket_id} завершён.")
    else:
        await update.effective_message.reply_text("У вас нет активного режима ответа.")

# ========= ЗАКРЫТИЕ СО СТОРОНЫ ПОЛЬЗОВАТЕЛЯ =========
async def cmd_close_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    uid = update.effective_user.id
    async with await adb() as conn:
        cur = await conn.execute(
            "SELECT ticket_id FROM tickets WHERE user_id=? AND status='open' ORDER BY id DESC LIMIT 1", (uid,))
        row = await cur.fetchone()
    if not row:
        await update.effective_message.reply_text("У вас нет открытых тикетов.")
        return
    ticket_id = row["ticket_id"]

    gids = await get_ticket_group_msg_ids(ticket_id)
    for mid in gids:
        try:
            await context.bot.delete_message(MOD_GROUP_ID, mid)
            await asyncio.sleep(0.03)
        except Exception:
            pass

    await close_ticket(ticket_id, None, None)
    await update.effective_message.reply_text(f"✅ Тикет {ticket_id} закрыт.")
    await context.bot.send_message(MOD_GROUP_ID, f"❌ Тикет {ticket_id} закрыт пользователем.")

# ========= ПАНЕЛЬ МОДЕРАЦИИ =========
async def cmd_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != MOD_GROUP_ID:
        return
    await update.effective_message.reply_text("⚙️ Панель управления", reply_markup=panel_keyboard())

async def cb_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.message.chat.id != MOD_GROUP_ID:
        await q.answer(); return
    parts = q.data.split(":")  # p:...
    await q.answer()

    if parts[1] == "stats":
        txt = await stats_text()
        await q.message.edit_text(txt, reply_markup=stats_keyboard())
        return

    if parts[1] == "history":
        ids = await last_tickets(limit=10)
        if not ids:
            await q.message.edit_text("Тикетов пока нет.", reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data="p:back")]]
            ))
            return
        await q.message.edit_text("📜 Выберите тикет:", reply_markup=history_menu_keyboard(ids))
        return

    if parts[1] == "autores":
        en = await autores_enabled()
        await q.message.edit_text("🤖 Настройки автоответчиков", reply_markup=autores_menu_keyboard(en))
        return

    if parts[1] == "status":
        now = dt.datetime.now().strftime("%H:%M:%S %d.%m.%Y")
        await q.message.reply_text(f"✅ Бот активен\n⏰ Серверное время: {now}")
        return

    if parts[1] == "back":
        await q.message.edit_text("⚙️ Панель управления", reply_markup=panel_keyboard())
        return

    if parts[1] == "stats" and len(parts) >= 3 and parts[2] == "refresh":
        txt = await stats_text()
        await q.message.edit_text(txt, reply_markup=stats_keyboard())
        return

    if parts[1] == "history" and len(parts) >= 3 and parts[2] == "show":
        t_id = parts[3]
        if not await ticket_exists(t_id):
            await q.message.reply_text("Тикет не найден.")
            return
        txt = await ticket_history_text(t_id, limit=50)
        await q.message.reply_text(txt)
        return

# ========= АВТООТВЕТЧИКИ (КНОПКИ) =========
async def cb_autores(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.message.chat.id != MOD_GROUP_ID:
        await q.answer(); return
    parts = q.data.split(":")  # ar:...
    await q.answer()

    if parts[1] == "toggle":
        en = await autores_enabled()
        await set_autores_enabled(not en)
        en2 = await autores_enabled()
        try:
            await q.message.edit_reply_markup(reply_markup=autores_menu_keyboard(en2))
        except Exception:
            pass
        return

    if parts[1] == "cat":
        cat = parts[2]
        cur = await get_autoresponder_text(cat) or "— не задан —"
        await q.message.reply_text(
            f"{CAT_TITLES_RU.get(cat, cat)}\n\nТекущий автоответ:\n{cur}",
            reply_markup=autores_cat_keyboard(cat)
        )
        return

    if parts[1] == "edit":
        cat = parts[2]
        context.chat_data["edit_autores_cat"] = cat
        await q.message.reply_text(f"✏️ Отправьте новый текст автоответа для: {CAT_TITLES_RU.get(cat, cat)}")
        return

# ввод нового текста автоответчика (в группе)
async def mod_group_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != MOD_GROUP_ID:
        return
    cat = context.chat_data.get("edit_autores_cat")
    if not cat:
        return
    text = update.effective_message.text or ""
    await set_autoresponder_text(cat, text)
    context.chat_data.pop("edit_autores_cat", None)
    await update.effective_message.reply_text("✅ Текст автоответа обновлён.")

# ========= ИСТОРИЯ/СТАТИСТИКА КОМАНДАМИ =========
async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != MOD_GROUP_ID:
        return
    if not context.args:
        await update.effective_message.reply_text("Использование: /history <TICKET_ID>")
        return
    t_id = context.args[0]
    if not await ticket_exists(t_id):
        await update.effective_message.reply_text("Тикет не найден.")
        return
    txt = await ticket_history_text(t_id, limit=50)
    await update.effective_message.reply_text(txt)

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != MOD_GROUP_ID:
        return
    txt = await stats_text()
    await update.effective_message.reply_text(txt)

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = dt.datetime.now().strftime("%H:%M:%S %d.%m.%Y")
    await update.effective_message.reply_text(f"✅ Бот работает\n⏰ Время сервера: {now}")

# ========= MAIN =========
async def main():
    await init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Пользователь
    app.add_handler(CommandHandler("start", cmd_start, filters.ChatType.PRIVATE))
    app.add_handler(CallbackQueryHandler(cb_lang, pattern="^lang:"))
    app.add_handler(CallbackQueryHandler(cb_category, pattern="^cat:"))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, pm_user_message))
    app.add_handler(CommandHandler("close", cmd_close_user, filters.ChatType.PRIVATE))

    # Модерация
    app.add_handler(CallbackQueryHandler(cb_ticket_actions, pattern="^t:"))
    app.add_handler(MessageHandler(filters.Chat(MOD_GROUP_ID) & ~filters.COMMAND, mod_group_message))
    app.add_handler(CommandHandler("end", cmd_end, filters.Chat(MOD_GROUP_ID)))
    app.add_handler(CommandHandler("panel", cmd_panel, filters.Chat(MOD_GROUP_ID)))
    app.add_handler(CallbackQueryHandler(cb_panel, pattern="^p:"))
    app.add_handler(CallbackQueryHandler(cb_autores, pattern="^ar:"))
    app.add_handler(MessageHandler(filters.Chat(MOD_GROUP_ID) & filters.TEXT, mod_group_text))
    app.add_handler(CommandHandler("history", cmd_history, filters.Chat(MOD_GROUP_ID)))
    app.add_handler(CommandHandler("stats", cmd_stats, filters.Chat(MOD_GROUP_ID)))
    app.add_handler(CommandHandler("status", cmd_status, filters.Chat(MOD_GROUP_ID)))

    print("🤖 Bot started and polling...")
    await app.run_polling(close_loop=False)

# ========= ЗАПУСК =========
import nest_asyncio
nest_asyncio.apply()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("❌ Bot stopped manually.")
