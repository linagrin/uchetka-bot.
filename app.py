# УЧЕТКА — Telegram-бот учёта рабочего времени
# Режим: 10:00–19:00, перерывы не вычитаются. Пн–Пт.
# Храним только текущий месяц; 1-го числа — саммари и чистка прошлого.
# Пятница до 19:00: предупреждение при жёстком недоборе.
# Форматы ввода времени: "10:30", "14.10.2025 10:30", "14.10 10:30".
# НОВОЕ:
# - /allweeks — баланс по ВСЕМ неделям текущего месяца + итог;
# - /week удалён;
# - /in, /out, /retro — разрешены ТОЛЬКО в рамках текущего месяца;
# - повторный /in или /out для той же даты ПЕРЕЗАПИСЫВАЕТ предыдущее значение за этот день.

import os
import asyncio
import re
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import asyncpg
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from aiohttp import web

# ---------- Константы ----------
TZ = ZoneInfo("Europe/Amsterdam")
START_T = time(10, 0)
END_T   = time(19, 0)
WORKDAYS = {0, 1, 2, 3, 4}
DAILY_NORM_MIN = 9 * 60
HARD_DEFICIT_MIN = 60

BOT_TOKEN = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]

DB_POOL: asyncpg.Pool | None = None

SQL_SCHEMA = """
CREATE TABLE IF NOT EXISTS users(
  user_id BIGINT PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS sessions(
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  start_ts TIMESTAMPTZ NOT NULL,
  end_ts   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_period ON sessions(start_ts, end_ts);

CREATE TABLE IF NOT EXISTS month_summaries(
  user_id BIGINT NOT NULL,
  year    INT NOT NULL,
  month   INT NOT NULL,
  sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(user_id, year, month)
);
"""

# ---------- Время ----------
def dlocal(dt: datetime) -> datetime:
    return dt.astimezone(TZ)

def local_now() -> datetime:
    return datetime.now(TZ)

def week_monday(dt: datetime) -> datetime:
    return (dt - timedelta(days=dt.weekday())).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=TZ)

def month_start(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=TZ)

def next_month_start(dt: datetime) -> datetime:
    first = month_start(dt)
    # прибавим 32 дня и вернёмся к 1-му
    tmp = first + timedelta(days=32)
    return tmp.replace(day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=TZ)

def prev_month_bounds(dt: datetime) -> tuple[datetime, datetime]:
    this0 = month_start(dt)
    prev_last = this0 - timedelta(days=1)
    prev0 = month_start(prev_last)
    return prev0, this0  # [prev0, this0)

def is_in_current_month(when: datetime, now: datetime) -> bool:
    return month_start(now) <= when < next_month_start(now)

# Парсер времени: "10:30"; "DD.MM.YYYY HH:MM"; "DD.MM HH:MM"
def parse_when(arg: str | None, now: datetime) -> datetime:
    if not arg or not arg.strip():
        return now
    s = arg.strip()
    m = re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", s)
    if m:
        hh, mm = map(int, m.groups())
        return now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    m = re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})\s+([01]\d|2[0-3]):([0-5]\d)", s)
    if m:
        dd, mo, yy, hh, mm = map(int, m.groups())
        return datetime(yy, mo, dd, hh, mm, tzinfo=TZ)
    m = re.fullmatch(r"(\d{2})\.(\d{2})\s+([01]\d|2[0-3]):([0-5]\d)", s)
    if m:
        dd, mo, hh, mm = map(int, m.groups())
        return datetime(now.year, mo, dd, hh, mm, tzinfo=TZ)
    # поддержим ISO если вдруг пришлют
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})\s+([01]\d|2[0-3]):([0-5]\d)", s)
    if m:
        y, mo, dd, hh, mm = map(int, m.groups())
        return datetime(y, mo, dd, hh, mm, tzinfo=TZ)
    raise ValueError("Форматы: «10:30», «14.10.2025 10:30» или «14.10 10:30».")

# ---------- DB helpers ----------
async def ensure_user(conn: asyncpg.Connection, uid: int):
    await conn.execute("INSERT INTO users(user_id) VALUES($1) ON CONFLICT DO NOTHING", uid)

async def purge_before_current_month(conn: asyncpg.Connection, now: datetime):
    m0 = month_start(now)
    await conn.execute("DELETE FROM sessions WHERE start_ts < $1", m0)

# Последний /in за день — главный: перезаписываем старт
async def upsert_in_for_day(conn: asyncpg.Connection, uid: int, when: datetime) -> str:
    day0 = datetime(when.year, when.month, when.day, 0, 0, tzinfo=TZ)
    day1 = day0 + timedelta(days=1)
    # ищем любые сессии этого дня
    row_open = await conn.fetchrow("""
        SELECT id FROM sessions
        WHERE user_id=$1 AND end_ts IS NULL AND start_ts >= $2 AND start_ts < $3
        ORDER BY id DESC LIMIT 1
    """, uid, day0, day1)
    if row_open:
        await conn.execute("UPDATE sessions SET start_ts=$1 WHERE id=$2", when, row_open["id"])
        return f"Приход обновлён: {when.strftime('%d.%m.%Y %H:%M')}"
    row_any = await conn.fetchrow("""
        SELECT id FROM sessions
        WHERE user_id=$1 AND start_ts >= $2 AND start_ts < $3
        ORDER BY start_ts DESC LIMIT 1
    """, uid, day0, day1)
    if row_any:
        await conn.execute("UPDATE sessions SET start_ts=$1 WHERE id=$2", when, row_any["id"])
        return f"Приход обновлён: {when.strftime('%d.%m.%Y %H:%M')}"
    # иначе создаём новую сессию этого дня
    await conn.execute("INSERT INTO sessions(user_id, start_ts) VALUES($1,$2)", uid, when)
    return f"Старт {when.strftime('%d.%m.%Y %H:%M')}"

# Последний /out за день — главный: перезаписываем финиш
async def set_out_for_day(conn: asyncpg.Connection, uid: int, when: datetime) -> str:
    day0 = datetime(when.year, when.month, when.day, 0, 0, tzinfo=TZ)
    day1 = day0 + timedelta(days=1)
    # сначала пробуем закрыть открытую сессию этого дня
    row_open = await conn.fetchrow("""
        SELECT id, start_ts FROM sessions
        WHERE user_id=$1 AND end_ts IS NULL AND start_ts >= $2 AND start_ts < $3
        ORDER BY id DESC LIMIT 1
    """, uid, day0, day1)
    if row_open:
        start_ts = dlocal(row_open["start_ts"])
        if when < start_ts:
            return "Время ухода раньше времени прихода. Укажи корректное время."
        await conn.execute("UPDATE sessions SET end_ts=$1 WHERE id=$2", when, row_open["id"])
        return f"Финиш {when.strftime('%d.%m.%Y %H:%M')} (начал(а) {start_ts.strftime('%d.%m.%Y %H:%M')})"
    # если открытой нет — ищем любую сессию этого дня и обновляем её финиш
    row_any = await conn.fetchrow("""
        SELECT id, start_ts, end_ts FROM sessions
        WHERE user_id=$1 AND start_ts >= $2 AND start_ts < $3
        ORDER BY COALESCE(end_ts, start_ts) DESC LIMIT 1
    """, uid, day0, day1)
    if row_any:
        start_ts = dlocal(row_any["start_ts"])
        if when < start_ts:
            return "Время ухода раньше времени прихода. Укажи корректное время."
        await conn.execute("UPDATE sessions SET end_ts=$1 WHERE id=$2", when, row_any["id"])
        return f"Финиш обновлён: {when.strftime('%d.%m.%Y %H:%M')} (начал(а) {start_ts.strftime('%d.%m.%Y %H:%M')})"
    return "Нет сессии для этого дня. Сначала /in для этой даты."

async def undo_last(conn: asyncpg.Connection, uid: int) -> str:
    row = await conn.fetchrow("SELECT id, start_ts, end_ts FROM sessions WHERE user_id=$1 ORDER BY id DESC LIMIT 1", uid)
    if not row:
        return "Нечего отменять."
    await conn.execute("DELETE FROM sessions WHERE id=$1", row["id"])
    s = dlocal(row["start_ts"]).strftime("%d.%m.%Y %H:%M")
    e = dlocal(row["end_ts"]).strftime("%d.%m.%Y %H:%M") if row["end_ts"] else "…"
    return f"Последняя сессия удалена: {s}–{e}"

# ---------- Расчёты ----------
def _deviation_minutes(first_in: datetime|None, last_out: datetime|None, day: datetime) -> int:
    t0 = datetime.combine(day.date(), START_T, TZ)
    t1 = datetime.combine(day.date(), END_T, TZ)
    minutes = 0
    if first_in:
        minutes += -int((first_in - t0).total_seconds() // 60) if first_in > t0 else int((t0 - first_in).total_seconds() // 60)
    if last_out:
        minutes += int((last_out - t1).total_seconds() // 60) if last_out > t1 else -int((t1 - last_out).total_seconds() // 60)
    return minutes

def _actual_minutes(first_in: datetime|None, last_out: datetime|None) -> int:
    if first_in and last_out:
        return max(0, int((last_out - first_in).total_seconds() // 60))
    return 0

async def _day_edges(conn: asyncpg.Connection, uid: int, day: datetime) -> tuple[datetime|None, datetime|None, list[str]]:
    day_start = datetime(day.year, day.month, day.day, 0, 0, tzinfo=TZ)
    day_end   = day_start + timedelta(days=1)
    rows = await conn.fetch(
        """
        SELECT start_ts, end_ts FROM sessions
        WHERE user_id=$1
          AND start_ts < $2
          AND (end_ts IS NULL OR end_ts >= $3)
        ORDER BY start_ts
        """,
        uid, day_end, day_start
    )
    starts, ends, seg = [], [], []
    for r in rows:
        s = dlocal(r["start_ts"])
        e = dlocal(r["end_ts"]) if r["end_ts"] else None
        if day_start <= s < day_end:
            starts.append(s)
        if e and (day_start <= e < day_end):
            ends.append(e)
        seg.append(f"• {s.strftime('%H:%M')}–{(e.strftime('%H:%M') if e else '…')}")
    first_in = min(starts) if starts else None
    last_out = max(ends) if ends else None
    return first_in, last_out, seg

async def day_deviation(conn: asyncpg.Connection, uid: int, day: datetime) -> tuple[int, list[str]]:
    first_in, last_out, seg = await _day_edges(conn, uid, day)
    dev = _deviation_minutes(first_in, last_out, day.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=TZ))
    return dev, seg

async def day_actual(conn: asyncpg.Connection, uid: int, day: datetime) -> int:
    first_in, last_out, _ = await _day_edges(conn, uid, day)
    return _actual_minutes(first_in, last_out)

async def week_deviation(conn: asyncpg.Connection, uid: int, any_dt: datetime) -> int:
    mon = week_monday(any_dt)
    total = 0
    for i in range(7):
        d = mon + timedelta(days=i)
        if d.weekday() not in WORKDAYS:
            continue
        dev, _ = await day_deviation(conn, uid, d)
        total += dev
    return total

def deficit_alert(now: datetime, balance_minutes: int) -> str:
    if now.weekday() == 4 and now.time() < END_T and balance_minutes < 0 and abs(balance_minutes) >= HARD_DEFICIT_MIN:
        return f"У тебя жесткий недобор по времени. Будь молодцом и досиди — {abs(balance_minutes)} минут"
    return ""

async def month_summary_text(conn: asyncpg.Connection, uid: int, first_prev: datetime, first_this: datetime) -> str|None:
    days_worked = 0
    minutes_worked = 0
    d = first_prev
    while d < first_this:
        mins = await day_actual(conn, uid, d)
        if mins > 0:
            days_worked += 1
            minutes_worked += mins
        d += timedelta(days=1)
    if days_worked == 0 and minutes_worked == 0:
        return None
    expected_workdays = sum(1 for i in range((first_this - first_prev).days) if (first_prev + timedelta(days=i)).weekday() in WORKDAYS)
    expected_minutes = expected_workdays * DAILY_NORM_MIN
    fact_hours = minutes_worked // 60
    fact_days = days_worked
    diff_min = minutes_worked - expected_minutes
    diff_hours_abs = abs(diff_min) // 60
    days_diff = abs(expected_workdays - fact_days)
    if diff_min >= 0:
        return (
            f"Ты отработал в этом месяце {fact_days} дней {fact_hours} часов\n"
            f"Это на {days_diff} дней {diff_hours_abs} часов больше, чем должно быть \n"
            f"Так держать! Никто кроме меня тебя за это не похвалит, так что радуйся!"
        )
    else:
        return (
            f"Ты отработал в этом месяце {fact_days} дней {fact_hours} часов\n"
            f"Это на {days_diff} дней {diff_hours_abs} часов меньше, чем должно быть \n"
            f"Постарайся не допускать такого в следующем месяце"
        )

# ---------- Ежемесячный планировщик ----------
async def send_month_summaries(bot: Bot, pool: asyncpg.Pool):
    while True:
        try:
            now = local_now()
            if now.day == 1 and time(0,5) <= now.time() <= time(0,30):
                prev0, this0 = prev_month_bounds(now)
                async with pool.acquire() as conn:
                    uids = [r["user_id"] for r in await conn.fetch("SELECT user_id FROM users")]
                    for uid in uids:
                        exists = await conn.fetchrow(
                            "SELECT 1 FROM month_summaries WHERE user_id=$1 AND year=$2 AND month=$3",
                            uid, prev0.year, prev0.month
                        )
                        if exists:
                            continue
                        text = await month_summary_text(conn, uid, prev0, this0)
                        if text:
                            await bot.send_message(chat_id=uid, text=text)
                        await conn.execute(
                            "INSERT INTO month_summaries(user_id, year, month) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
                            uid, prev0.year, prev0.month
                        )
                    await purge_before_current_month(conn, now)
        except Exception as e:
            print("scheduler error:", e)
        await asyncio.sleep(600)

# ---------- Health ----------
async def run_health_server():
    async def handle(_):
        return web.Response(text="ok")
    app = web.Application()
    app.router.add_get("/healthz", handle)
    port = int(os.getenv("PORT", "8000"))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

# ---------- Бот ----------
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Короткий /start — без простыней
@dp.message(Command("start"))
async def cmd_start(m: Message):
    now = local_now()
    async with DB_POOL.acquire() as conn:
        await ensure_user(conn, m.from_user.id)
        await purge_before_current_month(conn, now)
    await m.answer("Твой аккаунт активирован. Приятного пользования! Не забывай отмечаться.")

@dp.message(Command("help"))
async def cmd_help(m: Message):
    await m.answer(
        "Подсказки:\n"
        "• /in — приход (можно: «/in 10:30» или «/in 14.10.2025 10:30»)\n"
        "• /out — уход (можно с временем, как выше)\n"
        "• /retro 14.10.2025 09:50 in|out — задним числом (только текущий месяц)\n"
        "• /undo — отмена последней отметки\n"
        "• /status — баланс текущей недели\n"
        "• /day — отчёт за сегодня (можно дату: «/day 14.10.2025»)\n"
        "• /allweeks — балансы по всем неделям текущего месяца + итог\n"
        "\nПравила: 10:00–19:00, обед не вычитается. Данные — только текущий месяц."
    )

# --- Команды фиксации ---
@dp.message(Command("in"))
async def cmd_in(m: Message):
    parts = (m.text or "").split(maxsplit=1)
    now = local_now()
    try:
        when = parse_when(parts[1] if len(parts) > 1 else None, now)
    except ValueError as e:
        await m.answer(str(e)); return
    if not is_in_current_month(when, now):
        await m.answer("Отметки можно вносить только в рамках текущего месяца."); return
    async with DB_POOL.acquire() as conn:
        await ensure_user(conn, m.from_user.id)
        await purge_before_current_month(conn, now)
        txt = await upsert_in_for_day(conn, m.from_user.id, when)
        bal = await week_deviation(conn, m.from_user.id, when)
    sign = "+" if bal >= 0 else ""
    note = deficit_alert(when, bal)
    await m.answer(f"{txt}\nБаланс недели: {sign}{bal} мин." + (f"\n{note}" if note else ""))

@dp.message(Command("out"))
async def cmd_out(m: Message):
    parts = (m.text or "").split(maxsplit=1)
    now = local_now()
    try:
        when = parse_when(parts[1] if len(parts) > 1 else None, now)
    except ValueError as e:
        await m.answer(str(e)); return
    if not is_in_current_month(when, now):
        await m.answer("Отметки можно вносить только в рамках текущего месяца."); return
    async with DB_POOL.acquire() as conn:
        await purge_before_current_month(conn, now)
        txt = await set_out_for_day(conn, m.from_user.id, when)
        bal = await week_deviation(conn, m.from_user.id, when)
    sign = "+" if bal >= 0 else ""
    note = deficit_alert(when, bal)
    await m.answer(f"{txt}\nБаланс недели: {sign}{bal} мин." + (f"\n{note}" if note else ""))

@dp.message(Command("retro"))
async def cmd_retro(m: Message):
    # /retro 14.10.2025 09:50 in|out — ТОЛЬКО текущий месяц
    parts = (m.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await m.answer("Формат: /retro 14.10.2025 09:50 in|out"); return
    try:
        dt_part, kind = parts[1].rsplit(" ", 1)
        when = parse_when(dt_part, local_now())
        kind = kind.lower()
        if kind not in {"in", "out"}:
            raise ValueError
    except Exception:
        await m.answer("Формат: /retro 14.10.2025 09:50 in|out"); return
    now = local_now()
    if not is_in_current_month(when, now):
        await m.answer("Отметки можно вносить только в рамках текущего месяца."); return
    async with DB_POOL.acquire() as conn:
        await ensure_user(conn, m.from_user.id)
        await purge_before_current_month(conn, now)
        if kind == "in":
            txt = await upsert_in_for_day(conn, m.from_user.id, when)
        else:
            txt = await set_out_for_day(conn, m.from_user.id, when)
        bal = await week_deviation(conn, m.from_user.id, when)
    sign = "+" if bal >= 0 else ""
    await m.answer(f"{txt}\nБаланс недели: {sign}{bal} мин.")

@dp.message(Command("undo"))
async def cmd_undo(m: Message):
    now = local_now()
    async with DB_POOL.acquire() as conn:
        msg = await undo_last(conn, m.from_user.id)
        bal = await week_deviation(conn, m.from_user.id, now)
    sign = "+" if bal >= 0 else ""
    await m.answer(f"{msg}\nТекущий баланс недели: {sign}{bal} мин.")

# --- Отчёты ---
@dp.message(Command("status"))
async def cmd_status(m: Message):
    now = local_now()
    async with DB_POOL.acquire() as conn:
        await purge_before_current_month(conn, now)
        bal = await week_deviation(conn, m.from_user.id, now)
        row = await conn.fetchrow(
            "SELECT start_ts FROM sessions WHERE user_id=$1 AND end_ts IS NULL ORDER BY id DESC LIMIT 1",
            m.from_user.id
        )
    state = f"в работе с {dlocal(row['start_ts']).strftime('%H:%M')}" if row else "не в работе"
    sign = "+" if bal >= 0 else ""
    note = deficit_alert(now, bal)
    await m.answer(f"Статус: {state}\nБаланс недели: {sign}{bal} мин." + (f"\n{note}" if note else ""))

@dp.message(Command("day"))
async def cmd_day(m: Message):
    arg = (m.text or "").split(maxsplit=1)
    if len(arg) == 1:
        dt = local_now()
    else:
        s = arg[1].strip()
        m1 = re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})", s)
        if m1:
            dd, mo, yy = map(int, m1.groups())
            dt = datetime(yy, mo, dd, tzinfo=TZ)
        else:
            dt = datetime.fromisoformat(s).replace(tzinfo=TZ)
    async with DB_POOL.acquire() as conn:
        await purge_before_current_month(conn, dt)
        mins, seg = await day_deviation(conn, m.from_user.id, dt)
    sign = "+" if mins >= 0 else ""
    lines = "\n".join(seg) if seg else "нет отметок"
    await m.answer(f"День {dt.strftime('%d.%m.%Y')}: {sign}{mins} мин\n{lines}")

@dp.message(Command("allweeks"))
async def cmd_allweeks(m: Message):
    now = local_now()
    m0 = month_start(now)
    m1 = next_month_start(now)
    # собираем по неделям (ключ — понедельник недели)
    buckets = {}
    async with DB_POOL.acquire() as conn:
        d = m0
        while d < m1:
            if d.weekday() in WORKDAYS:
                dev, _ = await day_deviation(conn, m.from_user.id, d)
                mon = week_monday(d)
                buckets[mon] = buckets.get(mon, 0) + dev
            d += timedelta(days=1)
    if not buckets:
        await m.answer(f"{m0.strftime('%B %Y').capitalize()}: пока нет данных.")
        return
    # отсортируем по неделям и пронумеруем в пределах месяца
    weeks = sorted(buckets.items(), key=lambda kv: kv[0])
    # фильтруем только недели, которые пересекают текущий месяц
    weeks = [kv for kv in weeks if kv[0] < m1 and kv[0] + timedelta(days=6) >= m0]
    total = sum(v for _, v in weeks)
    lines = []
    for idx, (_, minutes) in enumerate(weeks, start=1):
        sign = "+" if minutes >= 0 else ""
        lines.append(f"{idx} неделя: {sign}{minutes} мин")
    sign_total = "+" if total >= 0 else ""
    month_title = m0.strftime('%B %Y').capitalize()
    text = f"{month_title}\n" + "\n".join(lines) + f"\nИтого: {sign_total}{total} мин"
    await m.answer(text)

# ---------- Точка входа ----------
async def main():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with DB_POOL.acquire() as conn:
        await conn.execute(SQL_SCHEMA)
    asyncio.create_task(run_health_server())
    asyncio.create_task(send_month_summaries(bot, DB_POOL))
    print("Uchetka bot started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
