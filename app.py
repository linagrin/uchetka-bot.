# УЧЕТКА — общий Telegram-бот учёта рабочего времени
# Режим: 10:00–19:00. Обеды/перерывы НЕ вычитаются.
# Итог дня (для баланса недели): (приход−10:00) +/- (уход−19:00)
# Обнуление данных: в НАЧАЛЕ нового месяца (храним только текущий месяц).
# Еженедельное предупреждение по пятницам до 19:00 при "жестком" недоборе — сохраняем.
# Авто-саммари в начале нового месяца по прошлому месяцу:
#   — "Ты отработал в этом месяце ... дней ... часов ..."
#   — сравнение с нормой (рабочие дни Пн–Пт * 9 часов/день).

import os
import asyncio
import re
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import asyncpg
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

TZ = ZoneInfo("Europe/Amsterdam")
START_T = time(10, 0)
END_T   = time(19, 0)
WORKDAYS = {0,1,2,3,4}   # Пн–Пт
DAILY_NORM_MIN = 9 * 60  # 9 часов в минутах
HARD_DEFICIT_MIN = 60    # порог "жесткого недобора" за неделю (минуты)

BOT_TOKEN = os.environ["BOT_TOKEN"]           # задать в Render
DATABASE_URL = os.environ["DATABASE_URL"]     # Render Postgres

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

-- фиксируем факт отправки месячного саммари, чтобы не дублировать
CREATE TABLE IF NOT EXISTS month_summaries(
  user_id BIGINT NOT NULL,
  year    INT NOT NULL,
  month   INT NOT NULL,
  sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(user_id, year, month)
);
"""

def dlocal(dt: datetime) -> datetime:
    return dt.astimezone(TZ)

def local_now() -> datetime:
    return datetime.now(TZ)

def week_monday(dt: datetime) -> datetime:
    return (dt - timedelta(days=dt.weekday())).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=TZ)

def month_start(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=TZ)

def prev_month_bounds(dt: datetime) -> tuple[datetime, datetime]:
    first_this = month_start(dt)
    last_prev_day = first_this - timedelta(days=1)
    first_prev = month_start(last_prev_day)
    return first_prev, first_this  # [first_prev, first_this)

async def ensure_user(conn: asyncpg.Connection, uid: int):
    await conn.execute("INSERT INTO users(user_id) VALUES($1) ON CONFLICT DO NOTHING", uid)

async def purge_before_current_month(conn: asyncpg.Connection, now: datetime):
    """Удаляем все сессии, начавшиеся до начала ТЕКУЩЕГО месяца."""
    m0 = month_start(now)
    await conn.execute("DELETE FROM sessions WHERE start_ts < $1", m0)

async def open_session(conn: asyncpg.Connection, uid: int, now: datetime) -> str:
    row = await conn.fetchrow("SELECT id FROM sessions WHERE user_id=$1 AND end_ts IS NULL", uid)
    if row:
        return "У тебя уже есть открытая сессия. Сначала /out."
    await conn.execute("INSERT INTO sessions(user_id, start_ts) VALUES($1, $2)", uid, now)
    return f"Старт {now.strftime('%Y-%m-%d %H:%M')}"

async def close_session(conn: asyncpg.Connection, uid: int, now: datetime) -> str:
    row = await conn.fetchrow(
        "SELECT id, start_ts FROM sessions WHERE user_id=$1 AND end_ts IS NULL ORDER BY id DESC LIMIT 1", uid
    )
    if not row:
        return "Нет открытой сессии. Сначала /in."
    await conn.execute("UPDATE sessions SET end_ts=$1 WHERE id=$2", now, row["id"])
    started = dlocal(row["start_ts"]).strftime("%Y-%m-%d %H:%M")
    return f"Финиш {now.strftime('%Y-%m-%d %H:%M')} (начал(а) {started})"

def _deviation_minutes(first_in: datetime|None, last_out: datetime|None, day: datetime) -> int:
    """Дневное отклонение (для недельного баланса), входные dt в локальной TZ."""
    t0 = datetime.combine(day.date(), START_T, TZ)
    t1 = datetime.combine(day.date(), END_T, TZ)
    minutes = 0
    if first_in:
        minutes += -int((first_in - t0).total_seconds() // 60) if first_in > t0 else int((t0 - first_in).total_seconds() // 60)
    if last_out:
        minutes += int((last_out - t1).total_seconds() // 60) if last_out > t1 else -int((t1 - last_out).total_seconds() // 60)
    return minutes

def _actual_minutes(first_in: datetime|None, last_out: datetime|None) -> int:
    """Фактически отработанные минуты за день (обед не вычитаем).
       Берём интервал от первого прихода до последнего ухода."""
    if first_in and last_out:
        return max(0, int((last_out - first_in).total_seconds() // 60))
    return 0

async def _day_edges(conn: asyncpg.Connection, uid: int, day: datetime) -> tuple[datetime|None, datetime|None, list[str]]:
    """Возвращает (самый ранний приход, самый поздний уход, список сегментов) за календарный день."""
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
    """Если пятница до 19:00 и сильный недобор — вернуть предупреждение."""
    if now.weekday() == 4 and now.time() < END_T and balance_minutes < 0 and abs(balance_minutes) >= HARD_DEFICIT_MIN:
        return f"У тебя жесткий недобор по времени. Будь молодцом и досиди — {abs(balance_minutes)} минут"
    return ""

async def month_summary_text(conn: asyncpg.Connection, uid: int, first_prev: datetime, first_this: datetime) -> str|None:
    """Считает фактические дни/часы за прошлый месяц и сравнивает с нормой."""
    # фактические
    days_worked = 0
    minutes_worked = 0
    d = first_prev
    while d < first_this:
        if d.weekday() in {0,1,2,3,4,5,6}:  # считаем любой день с отметками, не только Пн–Пт
            mins = await day_actual(conn, uid, d)
            if mins > 0:
                days_worked += 1
                minutes_worked += mins
        d += timedelta(days=1)

    # если за месяц ни одной отметки — нет смысла слать
    if days_worked == 0 and minutes_worked == 0:
        return None

    # норма = число рабочих дней Пн–Пт * 9 часов
    expected_workdays = 0
    d = first_prev
    while d < first_this:
        if d.weekday() in WORKDAYS:
            expected_workdays += 1
        d += timedelta(days=1)
    expected_minutes = expected_workdays * DAILY_NORM_MIN

    # форматирование
    fact_hours = minutes_worked // 60
    fact_days = days_worked
    diff_min = minutes_worked - expected_minutes
    diff_sign = "больше" if diff_min > 0 else "меньше"
    diff_hours_abs = abs(diff_min) // 60

    if diff_min >= 0:
        return (
            f"Ты отработал в этом месяце {fact_days} дней {fact_hours} часов\n"
            f"Это на {expected_workdays - fact_days} дней {diff_hours_abs} часов больше, чем должно быть \n"
            f"Так держать! Никто кроме меня тебя за это не похвалит, так что радуйся!"
        )
    else:
        return (
            f"Ты отработал в этом месяце {fact_days} дней {fact_hours} часов\n"
            f"Это на {expected_workdays - fact_days} дней {diff_hours_abs} часов меньше, чем должно быть \n"
            f"Постарайся не допускать такого в следующем месяце"
        )

async def send_month_summaries(bot: Bot, pool: asyncpg.Pool):
    """Раз в 10 минут проверяем: если начало месяца — шлём саммари по прошлому, очищаем прошлые данные."""
    while True:
        try:
            now = local_now()
            # окно отправки: 1-е число месяца с 00:05 до 00:30 локального времени
            if now.day == 1 and time(0,5) <= now.time() <= time(0,30):
                first_prev, first_this = prev_month_bounds(now)
                async with pool.acquire() as conn:
                    # список всех пользователей, кто стартовал бота
                    uids = [r["user_id"] for r in await conn.fetch("SELECT user_id FROM users")]
                    for uid in uids:
                        # проверяем, не отправляли ли уже
                        exists = await conn.fetchrow(
                            "SELECT 1 FROM month_summaries WHERE user_id=$1 AND year=$2 AND month=$3",
                            uid, first_prev.year, first_prev.month
                        )
                        if exists:
                            continue
                        text = await month_summary_text(conn, uid, first_prev, first_this)
                        if text:
                            await bot.send_message(chat_id=uid, text=text)
                        # помечаем как отправленное
                        await conn.execute(
                            "INSERT INTO month_summaries(user_id, year, month) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
                            uid, first_prev.year, first_prev.month
                        )
                    # после рассылки — очищаем всё старше текущего месяца
                    await purge_before_current_month(conn, now)
        except Exception as e:
            # чтобы планировщик не падал навсегда
            print("scheduler error:", e)
        await asyncio.sleep(600)  # 10 минут
# --- Bot wiring ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.message(Command("start"))
async def cmd_start(m: Message):
    now = local_now()
    async with m.bot["db"].acquire() as conn:
        await ensure_user(conn, m.from_user.id)
        await purge_before_current_month(conn, now)
    await m.answer(
        "УЧЕТКА активирована. Команды:\n"
        "/in — приход\n"
        "/out — уход\n"
        "/status — баланс недели\n"
        "/day [YYYY-MM-DD] — отчёт за день\n"
        "/week [YYYY-Www] — отчёт за ISO-неделю\n"
        "Режим: 10:00–19:00, обед не вычитается. Данные храним только текущий месяц; в начале нового месяца придёт саммари по прошлому."
    )

@dp.message(Command("in"))
async def cmd_in(m: Message):
    now = local_now()
    async with m.bot["db"].acquire() as conn:
        await ensure_user(conn, m.from_user.id)
        await purge_before_current_month(conn, now)
        txt = await open_session(conn, m.from_user.id, now)
        bal = await week_deviation(conn, m.from_user.id, now)
    sign = "+" if bal >= 0 else ""
    note = deficit_alert(now, bal)
    extra = f"\n{note}" if note else ""
    await m.answer(f"{txt}\nБаланс недели: {sign}{bal} мин.{extra}")

@dp.message(Command("out"))
async def cmd_out(m: Message):
    now = local_now()
    async with m.bot["db"].acquire() as conn:
        await purge_before_current_month(conn, now)
        txt = await close_session(conn, m.from_user.id, now)
        bal = await week_deviation(conn, m.from_user.id, now)
    sign = "+" if bal >= 0 else ""
    note = deficit_alert(now, bal)
    extra = f"\n{note}" if note else ""
    await m.answer(f"{txt}\nБаланс недели: {sign}{bal} мин.{extra}")

@dp.message(Command("status"))
async def cmd_status(m: Message):
    now = local_now()
    async with m.bot["db"].acquire() as conn:
        await purge_before_current_month(conn, now)
        bal = await week_deviation(conn, m.from_user.id, now)
        row = await conn.fetchrow(
            "SELECT start_ts FROM sessions WHERE user_id=$1 AND end_ts IS NULL ORDER BY id DESC LIMIT 1",
            m.from_user.id
        )
    state = f"в работе с {dlocal(row['start_ts']).strftime('%H:%M')}" if row else "не в работе"
    sign = "+" if bal >= 0 else ""
    note = deficit_alert(now, bal)
    extra = f"\n{note}" if note else ""
    await m.answer(
        f"Статус: {state}\n"
        f"Баланс недели: {sign}{bal} мин.\n"
        f"Режим: 10:00–19:00 (без вычета обеда){extra}"
    )

@dp.message(Command("day"))
async def cmd_day(m: Message):
    arg = (m.text or "").split(maxsplit=1)
    dt = local_now() if len(arg) == 1 else datetime.fromisoformat(arg[1]).replace(tzinfo=TZ)
    async with m.bot["db"].acquire() as conn:
        await purge_before_current_month(conn, dt)
        mins, seg = await day_deviation(conn, m.from_user.id, dt)
    sign = "+" if mins >= 0 else ""
    lines = "\n".join(seg) if seg else "нет отметок"
    await m.answer(f"День {dt.strftime('%Y-%m-%d')}: {sign}{mins} мин\n{lines}")

@dp.message(Command("week"))
async def cmd_week(m: Message):
    arg = (m.text or "").split(maxsplit=1)
    if len(arg) == 1:
        ref = local_now()
        y, w, _ = ref.isocalendar()
    else:
        mobj = re.fullmatch(r"(\d{4})-W(\d{2})", arg[1].strip())
        if not mobj:
            await m.answer("Формат: /week YYYY-Www (пример: 2025-W42)")
            return
        y, w = map(int, mobj.groups())
        jan4 = datetime(y, 1, 4, tzinfo=TZ)
        ref = (jan4 - timedelta(days=jan4.weekday())) + timedelta(weeks=w - jan4.isocalendar().week)
    async with m.bot["db"].acquire() as conn:
        await purge_before_current_month(conn, ref)
        minutes = await week_deviation(conn, m.from_user.id, ref)
    sign = "+" if minutes >= 0 else ""
    await m.answer(f"Неделя {y}-W{w}: {sign}{minutes} мин (режим 10:00–19:00).")

async def main():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with pool.acquire() as conn:
        await conn.execute(SQL_SCHEMA)
    bot["db"] = pool
    # запускаем планировщик месячных саммари
    asyncio.create_task(send_month_summaries(bot, pool))
    print("Uchetka bot started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())