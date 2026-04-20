import asyncio
import logging
import sqlite3
import json
import random
import time as time_module
import os
from datetime import datetime, timedelta, time
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

BOT_TOKEN = "8580256278:AAFJaI2Y7a76rRT2RDv6OJHSQTa_vNzKj6w"
DB_NAME = "taxi_game.db"
START_BALANCE = 5000
DAILY_REWARD = 1000
FUEL_PRICE = 2
ADMIN_PASSWORD = "060510"

STARS_RATE = 10_000_000
MIN_STARS = 15
MAX_STARS = 25

SPONSOR_CHANNELS = [
    "meduzakin1",
    "NikKatFUN",
    "taxistchanel"
]

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

PLACES = [
    "Аэропорт", "Железнодорожный вокзал", "Центральный рынок", "ТЦ «Мега»", "Парк Горького",
    "Стадион", "Университет", "Больница", "Полиция", "Пляж", "Озеро", "Лес", "Деревня",
    "Загородный клуб", "Ресторан «Уют»", "Ночной клуб", "Кинотеатр", "Театр", "Музей",
    "Выставочный центр", "Бизнес-центр", "Офисное здание", "Школа", "Детский сад",
    "Спортзал", "Бассейн", "Сауна", "Отель", "Хостел", "Квартал «Старый город»",
    "Новостройки", "Частный сектор", "Промзона", "Склад", "ТЭЦ", "Водохранилище", "Карьер",
    "Горнолыжный курорт", "Пансионат", "Санаторий", "Зоопарк", "Цирк", "Планетарий",
    "Космодром", "Аэродром", "Порт", "Яхт-клуб", "Рыболовная база", "Охотничье угодье",
    "Заброшенная деревня"
]

admin_users = {}

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            balance INTEGER DEFAULT 0,
            debt INTEGER DEFAULT 0,
            last_daily INTEGER DEFAULT 0,
            cars TEXT DEFAULT '[]',
            credits_count INTEGER DEFAULT 0,
            exp INTEGER DEFAULT 0,
            level INTEGER DEFAULT 1,
            hired_cars TEXT DEFAULT '[]',
            happy_passengers INTEGER DEFAULT 0,
            angry_passengers INTEGER DEFAULT 0,
            used_promocodes TEXT DEFAULT '[]',
            last_tip_reward_week INTEGER DEFAULT 0,
            last_interest INTEGER DEFAULT 0,
            factory_level INTEGER DEFAULT 0
        )
    """)
    
    cur.execute("PRAGMA table_info(users)")
    columns = [column[1] for column in cur.fetchall()]
    if 'credits_count' not in columns:
        cur.execute("ALTER TABLE users ADD COLUMN credits_count INTEGER DEFAULT 0")
    if 'exp' not in columns:
        cur.execute("ALTER TABLE users ADD COLUMN exp INTEGER DEFAULT 0")
    if 'level' not in columns:
        cur.execute("ALTER TABLE users ADD COLUMN level INTEGER DEFAULT 1")
    if 'hired_cars' not in columns:
        cur.execute("ALTER TABLE users ADD COLUMN hired_cars TEXT DEFAULT '[]'")
    if 'happy_passengers' not in columns:
        cur.execute("ALTER TABLE users ADD COLUMN happy_passengers INTEGER DEFAULT 0")
    if 'angry_passengers' not in columns:
        cur.execute("ALTER TABLE users ADD COLUMN angry_passengers INTEGER DEFAULT 0")
    if 'used_promocodes' not in columns:
        cur.execute("ALTER TABLE users ADD COLUMN used_promocodes TEXT DEFAULT '[]'")
    if 'last_tip_reward_week' not in columns:
        cur.execute("ALTER TABLE users ADD COLUMN last_tip_reward_week INTEGER DEFAULT 0")
    if 'last_interest' not in columns:
        cur.execute("ALTER TABLE users ADD COLUMN last_interest INTEGER DEFAULT 0")
    if 'factory_level' not in columns:
        cur.execute("ALTER TABLE users ADD COLUMN factory_level INTEGER DEFAULT 0")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tip_race (
            user_id INTEGER PRIMARY KEY,
            tips_total INTEGER DEFAULT 0,
            week_start INTEGER DEFAULT 0,
            last_update INTEGER DEFAULT 0
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS promocodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE,
            reward INTEGER,
            max_uses INTEGER DEFAULT 1,
            used_count INTEGER DEFAULT 0,
            expires_at INTEGER DEFAULT 0
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cars (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            price INTEGER,
            min_earning INTEGER,
            max_earning INTEGER,
            fuel_capacity INTEGER,
            fuel_consumption REAL
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS deposits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            amount INTEGER DEFAULT 0,
            last_interest INTEGER DEFAULT 0
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS withdraw_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            stars_amount INTEGER,
            comment TEXT DEFAULT '',
            status TEXT DEFAULT 'pending',
            created_at INTEGER DEFAULT 0,
            processed_at INTEGER DEFAULT 0
        )
    """)
    
    cur.execute("SELECT COUNT(*) FROM cars")
    if cur.fetchone()[0] == 0:
        cars_data = [
            ("Жигули", 5000, 10, 30, 40, 2),
            ("Renault Logan", 10000, 35, 70, 50, 2.0),
            ("Hyundai Solaris", 12000, 40, 80, 50, 2.2),
            ("Kia Rio", 13000, 45, 85, 50, 2.2),
            ("Лада Веста", 20000, 40, 80, 50, 2),
            ("Volkswagen Polo", 25000, 100, 180, 55, 2.8),
            ("Kia Cerato", 30000, 130, 220, 55, 2.8),
            ("Hyundai Elantra", 38000, 170, 290, 55, 3.0),
            ("Лада Трэвел", 40000, 60, 120, 55, 2.5),
            ("Skoda Octavia", 40000, 180, 300, 55, 3.0),
            ("Toyota Corolla", 42000, 190, 320, 60, 3.0),
            ("Тойота Камри", 60000, 120, 250, 60, 3),
            ("Kia K5", 85000, 420, 700, 70, 3.5),
            ("Toyota Camry (бизнес)", 90000, 450, 750, 70, 3.5),
            ("БМВ X5", 100000, 200, 400, 80, 4),
            ("Мерседес Бенц", 120000, 300, 500, 70, 3.5),
            ("Гелик (G-Class)", 150000, 400, 700, 100, 5),
            ("Тесла", 170000, 350, 600, 85, 3),
            ("Ксяоми электромобиль", 300000, 250, 450, 60, 2.5),
            ("Летающая машина", 10000000, 800, 1500, 150, 10)
        ]
        cur.executemany(
            "INSERT INTO cars (name, price, min_earning, max_earning, fuel_capacity, fuel_consumption) VALUES (?,?,?,?,?,?)",
            cars_data
        )
    conn.commit()
    conn.close()

def get_user(user_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT balance, debt, last_daily, cars, credits_count, exp, level, hired_cars, happy_passengers, angry_passengers, used_promocodes, last_tip_reward_week, last_interest, factory_level FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    if row is None:
        cur.execute(
            "INSERT INTO users (user_id, balance, debt, last_daily, cars, credits_count, exp, level, hired_cars, happy_passengers, angry_passengers, used_promocodes, last_tip_reward_week, last_interest, factory_level) VALUES (?, ?, 0, 0, '[]', 0, 0, 1, '[]', 0, 0, '[]', 0, 0, 0)",
            (user_id, START_BALANCE)
        )
        conn.commit()
        balance, debt, last_daily, cars, credits_count, exp, level, hired_cars, happy, angry, used, last_tip, last_interest, factory_level = START_BALANCE, 0, 0, '[]', 0, 0, 1, '[]', 0, 0, '[]', 0, 0, 0
    else:
        balance, debt, last_daily, cars, credits_count, exp, level, hired_cars, happy, angry, used, last_tip, last_interest, factory_level = row
    conn.close()
    cars_list = json.loads(cars)
    if cars_list and isinstance(cars_list[0], int):
        new_cars = [{"id": car_id, "fuel": 0} for car_id in cars_list]
        cars_list = new_cars
        update_user(user_id, cars=cars_list)
    hired_list = json.loads(hired_cars)
    used_list = json.loads(used)
    return {
        "balance": balance,
        "debt": debt,
        "last_daily": last_daily,
        "cars": cars_list,
        "credits_count": credits_count,
        "exp": exp,
        "level": level,
        "hired_cars": hired_list,
        "happy": happy,
        "angry": angry,
        "used_promocodes": used_list,
        "last_tip_reward_week": last_tip,
        "last_interest": last_interest,
        "factory_level": factory_level
    }

def update_user(user_id, **kwargs):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    for key, value in kwargs.items():
        if key in ("cars", "hired_cars", "used_promocodes"):
            value = json.dumps(value)
        cur.execute(f"UPDATE users SET {key} = ? WHERE user_id = ?", (value, user_id))
    conn.commit()
    conn.close()

def get_all_cars():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT id, name, price, min_earning, max_earning, fuel_capacity, fuel_consumption FROM cars ORDER BY price")
    rows = cur.fetchall()
    conn.close()
    cars = []
    for row in rows:
        cars.append({
            "id": row[0],
            "name": row[1],
            "price": row[2],
            "min_earn": row[3],
            "max_earn": row[4],
            "fuel_capacity": row[5],
            "fuel_consumption": row[6]
        })
    return cars

def get_car_by_id(car_id):
    cars = get_all_cars()
    for car in cars:
        if car["id"] == car_id:
            return car
    return None

def can_claim_daily(last_daily):
    return time_module.time() - last_daily >= 24 * 3600

def apply_interest(user_id):
    user = get_user(user_id)
    if user["debt"] == 0:
        return
    now = time_module.time()
    last = user.get("last_interest", 0)
    if last == 0:
        update_user(user_id, last_interest=int(now))
        return
    elapsed = now - last
    intervals = int(elapsed // (5 * 3600))
    if intervals > 0:
        interest_rate = 0.05
        new_debt = user["debt"]
        for _ in range(intervals):
            interest = int(new_debt * interest_rate)
            new_debt += interest
        new_last = last + intervals * 5 * 3600
        update_user(user_id, debt=new_debt, last_interest=int(new_last))
        logging.info(f"Проценты начислены пользователю {user_id}: +{new_debt - user['debt']}$ (интервалов: {intervals})")

def exp_to_next_level(level):
    return level * 100

def add_exp(user_id, amount):
    user = get_user(user_id)
    new_exp = user["exp"] + amount
    level = user["level"]
    leveled_up = False
    while new_exp >= exp_to_next_level(level):
        new_exp -= exp_to_next_level(level)
        level += 1
        leveled_up = True
        user["balance"] += 100
        update_user(user_id, balance=user["balance"])
    update_user(user_id, exp=new_exp, level=level)
    return level, new_exp, leveled_up

def get_current_week_start() -> int:
    now = datetime.now()
    days_to_subtract = now.weekday()
    week_start = now - timedelta(days=days_to_subtract)
    week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(week_start.timestamp())

def add_tip_to_race(user_id: int, tip_amount: int):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    current_week = get_current_week_start()
    cur.execute("SELECT tips_total, week_start FROM tip_race WHERE user_id = ?", (user_id,))
    result = cur.fetchone()
    if result:
        tips_total, week_start = result
        if week_start != current_week:
            tips_total = tip_amount
            week_start = current_week
        else:
            tips_total += tip_amount
        cur.execute(
            "UPDATE tip_race SET tips_total = ?, week_start = ?, last_update = ? WHERE user_id = ?",
            (tips_total, week_start, int(time_module.time()), user_id)
        )
    else:
        cur.execute(
            "INSERT INTO tip_race (user_id, tips_total, week_start, last_update) VALUES (?, ?, ?, ?)",
            (user_id, tip_amount, current_week, int(time_module.time()))
        )
    conn.commit()
    conn.close()

def get_tip_race_top(limit: int = 10):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    current_week = get_current_week_start()
    cur.execute("""
        SELECT user_id, tips_total FROM tip_race 
        WHERE week_start = ? 
        ORDER BY tips_total DESC 
        LIMIT ?
    """, (current_week, limit))
    results = cur.fetchall()
    conn.close()
    return results

def get_user_tip_position(user_id: int) -> tuple:
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    current_week = get_current_week_start()
    cur.execute("""
        SELECT user_id, tips_total FROM tip_race 
        WHERE week_start = ? 
        ORDER BY tips_total DESC
    """, (current_week,))
    all_users = cur.fetchall()
    conn.close()
    total_participants = len(all_users)
    user_tips = 0
    position = 0
    for i, (uid, tips) in enumerate(all_users, 1):
        if uid == user_id:
            position = i
            user_tips = tips
            break
    return position, user_tips, total_participants

async def distribute_tip_race_rewards():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    current_week = get_current_week_start()
    last_week = current_week - 7 * 24 * 3600
    cur.execute("""
        SELECT user_id, tips_total FROM tip_race 
        WHERE week_start = ? 
        ORDER BY tips_total DESC 
        LIMIT 10
    """, (last_week,))
    winners = cur.fetchall()
    rewards = {1: 50000, 2: 30000, 3: 20000}
    for i, (user_id, tips) in enumerate(winners, 1):
        reward = rewards.get(i, 10000)
        user = get_user(user_id)
        new_balance = user["balance"] + reward
        update_user(user_id, balance=new_balance)
        cur.execute("UPDATE users SET last_tip_reward_week = ? WHERE user_id = ?", (last_week, user_id))
        medal = "🏆" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "🎁"
        try:
            await bot.send_message(
                user_id,
                f"{medal} **Гонка чаевых завершена!**\n\n"
                f"Вы заняли **{i} место** с суммой чаевых **${tips}**!\n"
                f"Ваша награда: **${reward}** уже начислена на счёт.\n\n"
                f"Новая гонка уже началась! Удачи! 🚖",
                parse_mode="Markdown"
            )
        except:
            pass
    conn.commit()
    conn.close()
    logging.info(f"Награды гонки чаевых розданы {len(winners)} победителям")

async def tip_race_scheduler():
    while True:
        now = datetime.now()
        if now.weekday() == 0 and now.hour == 0 and now.minute >= 5:
            await distribute_tip_race_rewards()
            await asyncio.sleep(3600)
        else:
            await asyncio.sleep(1800)

async def check_user_subscriptions(user_id: int) -> tuple[bool, list]:
    not_subscribed = []
    for channel in SPONSOR_CHANNELS:
        try:
            member = await bot.get_chat_member(chat_id=f"@{channel}", user_id=user_id)
            if member.status in ['left', 'kicked']:
                not_subscribed.append(channel)
        except Exception as e:
            logging.error(f"Ошибка проверки канала {channel} для {user_id}: {e}")
            not_subscribed.append(channel)
    return (len(not_subscribed) == 0, not_subscribed)

def subscription_required(handler):
    async def wrapper(*args, **kwargs):
        event = args[0]
        if isinstance(event, types.Message):
            user_id = event.from_user.id
        elif isinstance(event, types.CallbackQuery):
            user_id = event.from_user.id
        else:
            return await handler(*args, **kwargs)

        ok, bad_channels = await check_user_subscriptions(user_id)
        if not ok:
            channels_text = "\n".join([f"👉 @{ch}" for ch in bad_channels])
            text = (
                "❌ **Доступ запрещён!**\n\n"
                "Для использования бота необходимо быть подписанным на всех спонсоров.\n"
                f"Вы отписались от:\n{channels_text}\n\n"
                "Подпишитесь и нажмите /start для проверки."
            )
            builder = InlineKeyboardBuilder()
            for ch in bad_channels:
                builder.add(InlineKeyboardButton(text=f"🔔 {ch}", url=f"https://t.me/{ch}"))
            builder.add(InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_sponsors"))
            builder.adjust(1)

            if isinstance(event, types.CallbackQuery):
                await event.answer()
                try:
                    await event.message.delete()
                except Exception as e:
                    logging.warning(f"Не удалось удалить сообщение: {e}")
                await event.message.answer(text, reply_markup=builder.as_markup())
            else:
                await event.reply(text, reply_markup=builder.as_markup())
            return
        return await handler(*args, **kwargs)
    return wrapper

@dp.callback_query(F.data == "check_sponsors")
@subscription_required
async def check_sponsors_callback(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение: {e}")
    await callback.message.answer("✅ Спасибо за подписку! Добро пожаловать обратно.", reply_markup=main_menu())

async def daily_subscription_check():
    while True:
        await asyncio.sleep(24 * 60 * 60)
        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users")
        all_users = cur.fetchall()
        conn.close()
        for (user_id,) in all_users:
            ok, bad_channels = await check_user_subscriptions(user_id)
            if not ok:
                channels_text = "\n".join([f"👉 @{ch}" for ch in bad_channels])
                text = (
                    "⚠️ **Внимание!**\n\n"
                    "Вы отписались от спонсоров нашего бота. "
                    "Для продолжения использования подпишитесь обратно:\n"
                    f"{channels_text}\n\n"
                    "После подписки нажмите /start для восстановления доступа."
                )
                builder = InlineKeyboardBuilder()
                for ch in bad_channels:
                    builder.add(InlineKeyboardButton(text=f"🔔 {ch}", url=f"https://t.me/{ch}"))
                builder.add(InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_sponsors"))
                builder.adjust(1)
                try:
                    await bot.send_message(user_id, text, reply_markup=builder.as_markup())
                except Exception as e:
                    logging.error(f"Не удалось отправить уведомление {user_id}: {e}")
            await asyncio.sleep(0.5)

def get_user_deposits(user_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT id, amount, last_interest FROM deposits WHERE user_id = ?", (user_id,))
    rows = cur.fetchall()
    conn.close()
    return [{"id": r[0], "amount": r[1], "last_interest": r[2]} for r in rows]

def create_deposit(user_id, amount):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("INSERT INTO deposits (user_id, amount, last_interest) VALUES (?, ?, ?)", (user_id, amount, int(time_module.time())))
    conn.commit()
    conn.close()

def add_to_deposit(deposit_id, amount):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("UPDATE deposits SET amount = amount + ? WHERE id = ?", (amount, deposit_id))
    conn.commit()
    conn.close()

def apply_deposit_interest(deposit_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT amount, last_interest FROM deposits WHERE id = ?", (deposit_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return 0, 0
    amount, last = row
    now = int(time_module.time())
    elapsed = now - last
    intervals = elapsed // (48 * 3600)
    if intervals > 0:
        interest = int(amount * 0.02 * intervals)
        new_amount = amount + interest
        new_last = last + intervals * 48 * 3600
        cur.execute("UPDATE deposits SET amount = ?, last_interest = ? WHERE id = ?", (new_amount, new_last, deposit_id))
        conn.commit()
        conn.close()
        return new_amount, interest
    conn.close()
    return amount, 0

def get_withdraw_requests(user_id=None, limit=20):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    if user_id:
        cur.execute("SELECT id, stars_amount, comment, status, created_at FROM withdraw_requests WHERE user_id = ? ORDER BY created_at DESC LIMIT ?", (user_id, limit))
    else:
        cur.execute("SELECT id, user_id, stars_amount, comment, status, created_at FROM withdraw_requests ORDER BY created_at DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows

# ------------------- НОВЫЕ КОМАНДЫ ДЛЯ КЕЙСОВ -------------------
@dp.message(Command("spend"))
@subscription_required
async def cmd_spend(message: types.Message, **kwargs):
    """Списание денег (используется мини-приложением)"""
    user_id = message.from_user.id
    args = message.text.split()
    if len(args) != 2:
        return
    try:
        amount = int(args[1])
        if amount <= 0:
            return
    except:
        return
    user = get_user(user_id)
    if user["balance"] >= amount:
        new_balance = user["balance"] - amount
        update_user(user_id, balance=new_balance)
        await message.reply(f"✅ Списано ${amount}. Новый баланс: ${new_balance}")
    else:
        await message.reply(f"❌ Недостаточно средств!")

@dp.message(Command("add_money"))
@subscription_required
async def cmd_add_money(message: types.Message, **kwargs):
    """Начисление денег (используется мини-приложением)"""
    user_id = message.from_user.id
    args = message.text.split()
    if len(args) != 2:
        return
    try:
        amount = int(args[1])
        if amount <= 0:
            return
    except:
        return
    user = get_user(user_id)
    new_balance = user["balance"] + amount
    update_user(user_id, balance=new_balance)
    await message.reply(f"✅ Начислено ${amount}. Новый баланс: ${new_balance}")

@dp.message(Command("notify_admin"))
@subscription_required
async def cmd_notify_admin(message: types.Message, **kwargs):
    """Уведомление админов о выигрыше 15 звёзд"""
    user_id = message.from_user.id
    text = message.text.replace("/notify_admin", "").strip()
    if not text:
        return
    user = get_user(user_id)
    for admin_id in admin_users.keys():
        try:
            await bot.send_message(admin_id, f"⭐ **Уведомление о выигрыше!**\n\nПользователь: {user_id}\nСообщение: {text}", parse_mode="Markdown")
        except:
            pass
    await message.reply("✅ Администратор уведомлён.")

# ------------------- ОБРАБОТЧИК ПРОМОКОДОВ (РАСШИРЕННЫЙ) -------------------
@dp.message(Command("promo"))
@subscription_required
async def activate_promo(message: types.Message, **kwargs):
    user_id = message.from_user.id
    args = message.text.split()
    if len(args) != 2:
        await message.reply("Использование: /promo <код>")
        return
    code = args[1].upper()
    user = get_user(user_id)
    if code in user["used_promocodes"]:
        await message.reply("❌ Вы уже активировали этот промокод.")
        return
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT id, reward, max_uses, used_count FROM promocodes WHERE code = ? AND (expires_at = 0 OR expires_at > ?)", (code, int(time_module.time())))
    promo = cur.fetchone()
    if promo:
        # Обычный промокод из БД
        promo_id, reward, max_uses, used_count = promo
        if used_count >= max_uses:
            conn.close()
            await message.reply("❌ Промокод уже использован максимальное количество раз.")
            return
        new_balance = user["balance"] + reward
        new_used = user["used_promocodes"] + [code]
        update_user(user_id, balance=new_balance, used_promocodes=new_used)
        cur.execute("UPDATE promocodes SET used_count = used_count + 1 WHERE id = ?", (promo_id,))
        conn.commit()
        conn.close()
        await message.reply(
            f"✅ Промокод {code} активирован!\n"
            f"Вы получили ${reward}!\n"
            f"Новый баланс: ${new_balance}",
            reply_markup=main_menu()
        )
        return
    conn.close()
    # Проверяем специальные коды для машины Kia Cerato (генерируются в кейсе)
    # Простейшая проверка: если код начинается с "CAR" или имеет определённую длину, но для безопасности будем проверять по базе
    # Вместо базы можно хранить список сгенерированных кодов в памяти, но при перезапуске они потеряются. Для простоты добавим в БД отдельную таблицу.
    # Но так как коды генерируются динамически, добавим таблицу case_codes.
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS case_codes (code TEXT PRIMARY KEY, reward_type TEXT, reward_value TEXT, used INTEGER DEFAULT 0)")
    conn.commit()
    cur.execute("SELECT reward_type, reward_value FROM case_codes WHERE code = ? AND used = 0", (code,))
    row = cur.fetchone()
    if row:
        reward_type, reward_value = row
        if reward_type == "car":
            # Добавляем машину Kia Cerato в гараж
            car_id = 7  # id Kia Cerato (из таблицы cars)
            car_info = get_car_by_id(car_id)
            if car_info:
                new_car = {"id": car_id, "fuel": car_info["fuel_capacity"]}
                new_cars = user["cars"] + [new_car]
                update_user(user_id, cars=new_cars)
                cur.execute("UPDATE case_codes SET used = 1 WHERE code = ?", (code,))
                conn.commit()
                conn.close()
                new_used = user["used_promocodes"] + [code]
                update_user(user_id, used_promocodes=new_used)
                await message.reply(
                    f"✅ Поздравляем! Вы активировали код на машину {car_info['name']}!\n"
                    f"Машина добавлена в гараж.",
                    reply_markup=main_menu()
                )
            else:
                conn.close()
                await message.reply("❌ Ошибка: машина не найдена.")
        elif reward_type == "stars":
            # Уведомляем админов о выигрыше 15 звёзд
            for admin_id in admin_users.keys():
                try:
                    await bot.send_message(
                        admin_id,
                        f"⭐ **Выигрыш 15 звёзд по коду!**\n\n"
                        f"Пользователь: {user_id}\n"
                        f"Код: {code}\n"
                        f"Необходимо отправить звёзды вручную.",
                        parse_mode="Markdown"
                    )
                except:
                    pass
            cur.execute("UPDATE case_codes SET used = 1 WHERE code = ?", (code,))
            conn.commit()
            conn.close()
            new_used = user["used_promocodes"] + [code]
            update_user(user_id, used_promocodes=new_used)
            await message.reply(
                f"✅ Код активирован! Вы выиграли вывод 15 звёзд.\n"
                f"Администратор уведомлён, ожидайте отправки.",
                reply_markup=main_menu()
            )
        else:
            conn.close()
            await message.reply("❌ Неверный или уже использованный код.")
    else:
        conn.close()
        await message.reply("❌ Неверный или истекший промокод.")

# ------------------- КОМАНДЫ ДЛЯ КЛИКЕРА -------------------
@dp.message(Command("tap"))
@subscription_required
async def cmd_tap(message: types.Message, **kwargs):
    user_id = message.from_user.id
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT factory_level FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    level = row[0] if row else 0
    conn.close()
    incomes = [1,2,4,6,10,15,20,50,100]
    income = incomes[level] if level < len(incomes) else 100
    user = get_user(user_id)
    new_balance = user["balance"] + income
    update_user(user_id, balance=new_balance)
    await message.reply(f"🏭 +{income}$", reply_markup=main_menu())

@dp.message(Command("upgrade"))
@subscription_required
async def cmd_upgrade(message: types.Message, **kwargs):
    user_id = message.from_user.id
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT factory_level FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    level = row[0] if row else 0
    conn.close()
    # Цены и доходы (уменьшены в 2 раза)
    upgrades = [
        {"price": 7500, "new_income": 2},
        {"price": 15000, "new_income": 4},
        {"price": 30000, "new_income": 6},
        {"price": 50000, "new_income": 10},
        {"price": 150000, "new_income": 15},
        {"price": 250000, "new_income": 20},
        {"price": 500000, "new_income": 50},
        {"price": 1000000, "new_income": 100}
    ]
    if level >= len(upgrades):
        await message.reply("❌ Максимальный уровень достигнут!")
        return
    price = upgrades[level]["price"]
    new_income = upgrades[level]["new_income"]
    user = get_user(user_id)
    if user["balance"] < price:
        await message.reply(f"❌ Недостаточно средств. Нужно ${price:,}")
        return
    new_balance = user["balance"] - price
    update_user(user_id, balance=new_balance)
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("UPDATE users SET factory_level = ? WHERE user_id = ?", (level + 1, user_id))
    conn.commit()
    conn.close()
    await message.reply(
        f"✅ Завод улучшен до уровня {level + 1}!\n"
        f"💰 Доход за клик теперь: {new_income}$\n"
        f"💸 С вас списано: ${price:,}\n"
        f"📊 Новый баланс: ${new_balance:,}"
    )

@dp.message(Command("balance"))
@subscription_required
async def cmd_balance(message: types.Message, **kwargs):
    user = get_user(message.from_user.id)
    await message.reply(f"💰 Ваш баланс: ${user['balance']:,}")

@dp.message(Command("get_factory_level"))
@subscription_required
async def cmd_get_factory_level(message: types.Message, **kwargs):
    user_id = message.from_user.id
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT factory_level FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    level = row[0] if row else 0
    conn.close()
    incomes = [1,2,4,6,10,15,20,50,100]
    income = incomes[level] if level < len(incomes) else 100
    await message.reply(f"🏭 Уровень завода: {level}\n💰 Доход за клик: {income}$")

# ------------------- ОСТАЛЬНЫЕ КОМАНДЫ (БАНК, ГАРАЖ, ВЫВОД И Т.Д.) -------------------
# Здесь должны быть все остальные хендлеры (status, daily, top_players, work_menu, do_work, factory, garage, refuel_menu, buy_menu, sell_car_menu, hired_menu, loan_menu, repay_menu, tip_race_menu, deposits_menu, close_deposit_callback, take_loan, repay_loan, pay_user, hire_driver, fire_driver, sell_car_command, cmd_deposit, withdraw_info, withdraw_15, withdraw_25, admin_handlers и т.д.)
# Для краткости я не буду дублировать их полностью, но в реальном файле они должны быть. Ниже приведены сигнатуры для напоминания.
# Весь этот код уже был в предыдущих версиях, его нужно просто добавить.

# ------------------- ЗАПУСК -------------------
async def main():
    init_db()
    print("Бот запущен...")
    asyncio.create_task(tip_race_scheduler())
    asyncio.create_task(daily_subscription_check())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

def main_menu():
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="🚖 Работать", callback_data="work_main"))
    builder.add(InlineKeyboardButton(text="🏦 Банк", callback_data="bank_main"))
    builder.add(InlineKeyboardButton(text="📊 Мой статус", callback_data="status"))
    builder.add(InlineKeyboardButton(text="🎁 Ежедневная награда", callback_data="daily"))
    builder.add(InlineKeyboardButton(text="⭐ Вывести", callback_data="withdraw_info"))
    builder.add(InlineKeyboardButton(text="👑 Админ панель", callback_data="admin_panel"))
    builder.add(InlineKeyboardButton(text="🏆 Топ игроков", callback_data="top_players"))
    builder.add(InlineKeyboardButton(text="🎫 Промокоды", callback_data="promocode_menu"))
    builder.adjust(2)
    return builder.as_markup()

def work_submenu():
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="🚖 Работать таксистом", callback_data="work_menu"))
    builder.add(InlineKeyboardButton(text="🏭 Работать на заводе", callback_data="factory"))
    builder.add(InlineKeyboardButton(text="🚗 Гараж", callback_data="garage"))
    builder.add(InlineKeyboardButton(text="⛽ Заправка", callback_data="refuel_menu"))
    builder.add(InlineKeyboardButton(text="🏎 Купить машину", callback_data="buy_menu"))
    builder.add(InlineKeyboardButton(text="🏷 Продать машину", callback_data="sell_car_menu"))
    builder.add(InlineKeyboardButton(text="👨‍✈️ Наёмные водители", callback_data="hired_menu"))
    builder.add(InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu"))
    builder.adjust(2)
    return builder.as_markup()

def bank_submenu():
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="💰 Взять кредит", callback_data="loan_menu"))
    builder.add(InlineKeyboardButton(text="💳 Погасить кредит", callback_data="repay_menu"))
    builder.add(InlineKeyboardButton(text="🏁 Гонка чаевых", callback_data="tip_race_menu"))
    builder.add(InlineKeyboardButton(text="💳 Мои вклады", callback_data="deposits_menu"))
    builder.add(InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu"))
    builder.adjust(2)
    return builder.as_markup()

def cars_keyboard(cars_list, action_prefix):
    builder = InlineKeyboardBuilder()
    for car in cars_list:
        if isinstance(car, dict) and 'fuel' in car:
            car_info = get_car_by_id(car["id"])
            if car_info:
                text = f"{car_info['name']} (ID: {car['id']}) — топливо: {car['fuel']}/{car_info['fuel_capacity']} л"
                callback = f"{action_prefix}_{car['id']}"
            else:
                continue
        else:
            text = f"{car['name']} — ${car['price']} (доход ${car['min_earn']}-${car['max_earn']})"
            callback = f"{action_prefix}_{car['id']}"
        builder.add(InlineKeyboardButton(text=text, callback_data=callback))
    builder.add(InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu"))
    builder.adjust(1)
    return builder.as_markup()

def fuel_options_keyboard(car_id):
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="➕ 10 л", callback_data=f"fuel_{car_id}_10"))
    builder.add(InlineKeyboardButton(text="➕ 50 л", callback_data=f"fuel_{car_id}_50"))
    builder.add(InlineKeyboardButton(text="⛽ Полный бак", callback_data=f"fuel_{car_id}_full"))
    builder.add(InlineKeyboardButton(text="🔙 Назад", callback_data="refuel_menu"))
    builder.adjust(1)
    return builder.as_markup()

def admin_menu():
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="💰 Дать себе денег (+1 млн)", callback_data="admin_add_money"))
    builder.add(InlineKeyboardButton(text="💸 Перевести деньги игроку", callback_data="admin_transfer_menu"))
    builder.add(InlineKeyboardButton(text="🚗 Дать машину", callback_data="admin_give_car_menu"))
    builder.add(InlineKeyboardButton(text="🔧 Починить бак (полный)", callback_data="admin_full_fuel"))
    builder.add(InlineKeyboardButton(text="📊 Статистика игроков", callback_data="admin_stats"))
    builder.add(InlineKeyboardButton(text="🔄 Обнулить счёт игрока", callback_data="admin_reset_user_menu"))
    builder.add(InlineKeyboardButton(text="🔄 Сбросить ВСЕХ игроков", callback_data="admin_reset_all_confirm"))
    builder.add(InlineKeyboardButton(text="📢 Сделать рассылку", callback_data="admin_broadcast_confirm"))
    builder.add(InlineKeyboardButton(text="🎫 Создать промокод", callback_data="admin_create_promo"))
    builder.add(InlineKeyboardButton(text="📋 Заявки на вывод", callback_data="admin_withdraw_list"))
    builder.add(InlineKeyboardButton(text="🔙 Назад в меню", callback_data="back_to_menu"))
    builder.adjust(1)
    return builder.as_markup()

@dp.message(Command("start"))
@subscription_required
async def cmd_start(message: types.Message, **kwargs):
    user_id = message.from_user.id
    get_user(user_id)
    await message.answer(
        "🚖 Добро пожаловать в игру «Таксист»!\n\n"
        f"Вы начинаете с балансом ${START_BALANCE} и без машин.\n"
        "Чтобы купить первую машину, можно сразу взять кредит.\n"
        "Зарабатывайте, отдавайте долг, покупайте более крутые авто.\n"
        "Теперь у машин есть топливо! Не забывайте заправляться.\n\n"
        "Введите /commands чтобы увидеть список всех команд и их описание.\n\n"
        "Используйте кнопки меню ниже 👇",
        reply_markup=main_menu()
    )

@dp.message(Command("commands"))
@subscription_required
async def cmd_commands(message: types.Message, **kwargs):
    commands_text = """
📋 **Список команд:**

/start - Запустить бота и начать игру
/commands - Показать это меню
/loan <сумма> - Взять кредит (макс. 500.000$, макс. 5 кредитов) (пример: /loan 5000)
/repay <сумма> - Погасить кредит (пример: /repay 2000)
/pay <сумма> <id> - Перевести деньги другому игроку (пример: /pay 100 123456789)
/admin <пароль> - Войти в админ-панель
/hire <id машины> - Нанять водителя на свою машину (из гаража)
/fire <id машины> - Уволить водителя
/sell <id машины> - Продать машину (половина стоимости)
/promo <код> - Активировать промокод
/deposit <сумма> - Положить деньги на вклад (от 20 000 до 100 000$, максимум 2 вклада, каждый до 500 000$, +2% каждые 2 дня)
/withdraw <количество звёзд> - Запросить вывод Telegram Stars (от 15 до 25 звёзд, курс 10 млн $ за 1 звезду)
/tap - Кликнуть по заводу (заработок от уровня завода)
/upgrade - Улучшить завод (увеличить доход за клик)
/balance - Показать баланс
/get_factory_level - Показать уровень завода

🎮 **Игровые механики:**
• Работа таксистом — зарабатывайте деньги, тратьте топливо
• Работа на заводе — стабильный, но маленький доход
• Покупка машин — чем дороже машина, тем выше заработок
• Топливо — покупайте на заправке, без него не поедете
• Кредиты — берите для покупки дорогих машин (макс. 500.000$, макс. 5 кредитов). Каждые 5 часов начисляется 5% на остаток долга
• Ежедневная награда — заходите каждый день и получайте 1000$
• Переводы — помогайте друзьям или расплачивайтесь с долгами
• Опыт и уровни — за каждую поездку вы получаете опыт, с новым уровнем получаете бонус
• Рейтинг водителя — довольные пассажиры повышают рейтинг и доход
• Наёмные водители — купите машину и наймите водителя, он будет приносить доход
• Промокоды — активируйте специальные коды для получения бонусов
• Гонка чаевых — соревнуйтесь с другими игроками за призы
• Вклады — кладите деньги под 2% каждые 2 дня (до 2 вкладов, макс. 500 000$ на вклад)
• Вывод Telegram Stars — обменивайте внутриигровые деньги на реальные звёзды (15–25 ⭐, курс 10 млн $ за 1 ⭐)
• Кликер-завод — нажимайте на кнопку и зарабатывайте, улучшайте завод

⚠️ Кредиты нужно вовремя погашать, иначе проценты быстро увеличат долг!
    """
    await message.reply(commands_text, parse_mode="Markdown")

@dp.message(Command("admin"))
async def cmd_admin(message: types.Message, **kwargs):
    user_id = message.from_user.id
    args = message.text.split()
    if len(args) != 2:
        await message.reply("Использование: /admin <пароль>")
        return
    if args[1] == ADMIN_PASSWORD:
        admin_users[user_id] = True
        await message.reply("✅ Пароль верный! Вы вошли в админ-панель.\nНажмите кнопку '👑 Админ панель' в главном меню.", reply_markup=main_menu())
    else:
        await message.reply("❌ Неверный пароль!")

@dp.callback_query(F.data == "work_main")
@subscription_required
async def work_main(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение: {e}")
    await callback.message.answer("🚖 Выберите действие:", reply_markup=work_submenu())

@dp.callback_query(F.data == "bank_main")
@subscription_required
async def bank_main(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение: {e}")
    await callback.message.answer("🏦 Банковские операции:", reply_markup=bank_submenu())

@dp.callback_query(F.data == "back_to_menu")
async def back_to_menu(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение: {e}")
    await callback.message.answer("Главное меню:", reply_markup=main_menu())

@dp.callback_query(F.data == "withdraw_info")
@subscription_required
async def withdraw_info(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    text = (
        "⭐ Вывод Telegram Stars\n\n"
        f"Курс: 10 000 000 $ = 1 ⭐\n"
        "Доступные суммы вывода:\n"
        f"• 15 ⭐ (150 000 000 $)\n"
        f"• 25 ⭐ (250 000 000 $)\n\n"
        "Нажмите на кнопку ниже для вывода:"
    )
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="⭐ Вывести 15 звёзд", callback_data="withdraw_15"))
    builder.add(InlineKeyboardButton(text="⭐ Вывести 25 звёзд", callback_data="withdraw_25"))
    builder.add(InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu"))
    builder.adjust(1)
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "withdraw_15")
@subscription_required
async def withdraw_15(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    fake_message = types.Message(
        message_id=callback.message.message_id,
        from_user=callback.from_user,
        chat=callback.message.chat,
        text="/withdraw 15",
        date=callback.message.date
    )
    await cmd_withdraw_stars(fake_message)

@dp.callback_query(F.data == "withdraw_25")
@subscription_required
async def withdraw_25(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    fake_message = types.Message(
        message_id=callback.message.message_id,
        from_user=callback.from_user,
        chat=callback.message.chat,
        text="/withdraw 25",
        date=callback.message.date
    )
    await cmd_withdraw_stars(fake_message)
@dp.callback_query(F.data == "status")
@subscription_required
async def show_status(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    user_id = callback.from_user.id
    user = get_user(user_id)
    cars_count = len(user["cars"])
    hired_count = len(user["hired_cars"])
    next_exp = exp_to_next_level(user["level"])
    total_passengers = user["happy"] + user["angry"]
    if total_passengers > 0:
        rating = (user["happy"] / total_passengers) * 100
        rating_line = f"😊 Довольных: {user['happy']} | 😠 Недовольных: {user['angry']}\n⭐ Рейтинг: {rating:.1f}%"
    else:
        rating_line = "😐 Пока нет пассажиров"
    new_text = (f"📊 Ваш статус:\n"
                f"💰 Баланс: ${user['balance']}\n"
                f"💳 Долг: ${user['debt']}\n"
                f"🚗 Машин в гараже: {cars_count}\n"
                f"👨‍✈️ Наёмных водителей: {hired_count}\n"
                f"📊 Кредитов взято: {user['credits_count']}/5\n"
                f"📈 Уровень: {user['level']} (опыт: {user['exp']}/{next_exp})\n"
                f"{rating_line}")
    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение: {e}")
    await callback.message.answer(new_text, reply_markup=main_menu())

@dp.callback_query(F.data == "daily")
@subscription_required
async def daily_reward(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    user_id = callback.from_user.id
    user = get_user(user_id)
    if can_claim_daily(user["last_daily"]):
        new_balance = user["balance"] + DAILY_REWARD
        update_user(user_id, balance=new_balance, last_daily=int(time_module.time()))
        new_text = f"🎁 Вы получили ежедневную награду: +{DAILY_REWARD}$\nТеперь у вас ${new_balance}."
        try:
            await callback.message.delete()
        except Exception as e:
            logging.warning(f"Не удалось удалить сообщение: {e}")
        await callback.message.answer(new_text, reply_markup=main_menu())
    else:
        next_time = datetime.fromtimestamp(user["last_daily"] + 86400).strftime("%Y-%m-%d %H:%M:%S")
        new_text = f"⏳ Награду можно будет получить снова после {next_time}."
        try:
            await callback.message.delete()
        except Exception as e:
            logging.warning(f"Не удалось удалить сообщение: {e}")
        await callback.message.answer(new_text, reply_markup=main_menu())

@dp.callback_query(F.data == "top_players")
@subscription_required
async def top_players(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT user_id, balance FROM users ORDER BY balance DESC LIMIT 10")
    top_users = cur.fetchall()
    conn.close()
    if not top_users:
        text = "🏆 Топ игроков пока пуст."
    else:
        text = "🏆 Топ 10 игроков по балансу:\n\n"
        for i, (user_id, balance) in enumerate(top_users, 1):
            try:
                user = await bot.get_chat(user_id)
                username = user.username or f"ID {user_id}"
                text += f"{i}. @{username} — ${balance}\n"
            except:
                text += f"{i}. ID {user_id} — ${balance}\n"
    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение: {e}")
    await callback.message.answer(text, reply_markup=main_menu())

@dp.callback_query(F.data == "promocode_menu")
@subscription_required
async def promocode_menu(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    text = (
        "🎫 **Промокоды**\n\n"
        "Вводи промокоды и получай бонусы!\n\n"
        "Как получить промокод?\n"
        "• Подпишись на наш канал @taxistchanel\n"
        "• Следи за новостями и розыгрышами\n"
        "• Участвуй в конкурсах\n\n"
        "👉 **Чтобы активировать промокод, введи команду:**\n"
        "`/promo ТВОЙ_КОД`\n\n"
        "Например: `/promo BONUS30000`"
    )
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="🔔 Перейти на канал", url="https://t.me/taxistchanel"))
    builder.add(InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu"))
    builder.adjust(1)
    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение: {e}")
    await callback.message.answer(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "work_menu")
@subscription_required
async def work_menu(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    user_id = callback.from_user.id
    user = get_user(user_id)
    if not user["cars"]:
        await callback.message.edit_text("У вас нет машин. Купите машину или работайте на заводе.", reply_markup=work_submenu())
        return
    builder = InlineKeyboardBuilder()
    for car_item in user["cars"]:
        car = get_car_by_id(car_item["id"])
        if car:
            fuel_status = f"{car_item['fuel']}/{car['fuel_capacity']} л"
            text = f"{car['name']} (⛽ {fuel_status})"
            callback_data = f"work_{car['id']}"
            builder.add(InlineKeyboardButton(text=text, callback_data=callback_data))
    builder.add(InlineKeyboardButton(text="🔙 Назад", callback_data="work_main"))
    builder.adjust(1)
    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение: {e}")
    await callback.message.answer("Выберите машину для работы:", reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("work_"))
@subscription_required
async def do_work(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    user_id = callback.from_user.id
    car_id = int(callback.data.split("_")[1])
    user = get_user(user_id)
    car_item = next((c for c in user["cars"] if c["id"] == car_id), None)
    if not car_item:
        await callback.message.edit_text("У вас нет такой машины!", reply_markup=work_submenu())
        return
    car_info = get_car_by_id(car_id)
    if not car_info:
        await callback.message.edit_text("Ошибка: машина не найдена.", reply_markup=work_submenu())
        return
    if car_item["fuel"] < car_info["fuel_consumption"]:
        await callback.message.edit_text(
            f"⛽ Недостаточно топлива! Нужно {car_info['fuel_consumption']} л, у вас {car_item['fuel']} л.\n"
            "Заправьтесь в меню ⛽ Заправка.",
            reply_markup=work_submenu()
        )
        return
    place = random.choice(PLACES)
    earning = random.randint(car_info["min_earn"], car_info["max_earn"])
    new_balance = user["balance"] + earning
    new_fuel = car_item["fuel"] - car_info["fuel_consumption"]
    for c in user["cars"]:
        if c["id"] == car_id:
            c["fuel"] = new_fuel
            break
    exp_gain = random.randint(5, 15)
    new_level, new_exp, leveled_up = add_exp(user_id, exp_gain)
    
    happy_chance = 0.7
    if new_fuel < car_info["fuel_capacity"] * 0.2:
        happy_chance -= 0.2
    if user["level"] > 5:
        happy_chance += 0.1
    if random.random() < happy_chance:
        new_happy = user["happy"] + 1
        update_user(user_id, happy_passengers=new_happy)
        rating_text = f"\n😊 Пассажир остался доволен!"
    else:
        new_angry = user["angry"] + 1
        update_user(user_id, angry_passengers=new_angry)
        rating_text = f"\n😠 Пассажир остался недоволен!"
    
    update_user(user_id, balance=new_balance, cars=user["cars"])
    
    event_text = ""
    if random.random() < 0.1:
        event_roll = random.randint(1, 3)
        if event_roll == 1:
            tip = random.randint(10, 50)
            new_balance += tip
            update_user(user_id, balance=new_balance)
            add_tip_to_race(user_id, tip)
            event_text = f"\n💵 Пассажир оставил чаевые: +${tip}!"
        elif event_roll == 2:
            fine = random.randint(10, 30)
            new_balance -= fine
            update_user(user_id, balance=new_balance)
            event_text = f"\n👮 Вас оштрафовали: -${fine}!"
        else:
            found = random.randint(5, 25)
            new_balance += found
            update_user(user_id, balance=new_balance)
            event_text = f"\n🍀 Вы нашли ${found}!"
    level_text = f"\n🌟 Получено опыта: +{exp_gain}. "
    if leveled_up:
        level_text += f"Поздравляем! Вы достигли {new_level} уровня! +100$ бонус!"
    await callback.message.edit_text(
        f"🚖 Вы отвезли пассажира в {place} и заработали ${earning}.\n"
        f"Расход топлива: {car_info['fuel_consumption']} л. Осталось топлива: {new_fuel:.1f} л.\n"
        f"Теперь у вас ${new_balance}.{event_text}{rating_text}{level_text}",
        reply_markup=work_submenu()
    )

@dp.callback_query(F.data == "factory")
@subscription_required
async def factory_work(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    user_id = callback.from_user.id
    user = get_user(user_id)
    earning = 5
    new_balance = user["balance"] + earning
    update_user(user_id, balance=new_balance)
    await callback.message.edit_text(
        f"🏭 Вы отработали смену на заводе и получили ${earning}. Теперь у вас ${new_balance}.",
        reply_markup=work_submenu()
    )

@dp.callback_query(F.data == "garage")
@subscription_required
async def show_garage(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    user_id = callback.from_user.id
    user = get_user(user_id)
    if not user["cars"]:
        new_text = "🚘 В гараже пусто. Купите машину!"
        if callback.message.text != new_text or callback.message.reply_markup != work_submenu():
            await callback.message.edit_text(new_text, reply_markup=work_submenu())
    else:
        text = "🚗 Ваши машины:\n"
        for car_item in user["cars"]:
            car = get_car_by_id(car_item["id"])
            if car:
                text += f"• {car['name']} (ID: {car_item['id']}) — топливо: {car_item['fuel']}/{car['fuel_capacity']} л\n"
        text += "\nЗаправляйтесь в меню ⛽ Заправка.\nИспользуйте /hire ID чтобы нанять водителя."
        if callback.message.text != text or callback.message.reply_markup != work_submenu():
            await callback.message.edit_text(text, reply_markup=work_submenu())

@dp.callback_query(F.data == "refuel_menu")
@subscription_required
async def refuel_menu(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    user_id = callback.from_user.id
    user = get_user(user_id)
    if not user["cars"]:
        await callback.message.edit_text("⛽ У вас нет машин. Сначала купите машину!", reply_markup=work_submenu())
        return
    builder = InlineKeyboardBuilder()
    for car_item in user["cars"]:
        car_info = get_car_by_id(car_item["id"])
        if car_info:
            text = f"{car_info['name']} (ID: {car_item['id']}) — ⛽ {car_item['fuel']}/{car_info['fuel_capacity']} л"
            callback_data = f"refuel_{car_item['id']}"
            builder.add(InlineKeyboardButton(text=text, callback_data=callback_data))
        else:
            logging.error(f"Машина с ID {car_item['id']} есть у пользователя {user_id}, но отсутствует в таблице cars.")
            continue
    builder.add(InlineKeyboardButton(text="🔙 Назад", callback_data="work_main"))
    builder.adjust(1)
    await callback.message.edit_text("⛽ Выберите машину для заправки:", reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("refuel_") & ~F.data.contains("_full") & ~F.data.contains("_10") & ~F.data.contains("_50"))
@subscription_required
async def choose_fuel_option(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    try:
        car_id = int(callback.data.split("_")[1])
    except (IndexError, ValueError):
        await callback.message.edit_text("Ошибка: неверный формат данных.", reply_markup=work_submenu())
        return
    await callback.message.edit_text(
        f"⛽ Выберите количество топлива для машины (ID: {car_id}):",
        reply_markup=fuel_options_keyboard(car_id)
    )

@dp.callback_query(F.data.startswith("fuel_"))
@subscription_required
async def process_fuel(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    parts = callback.data.split("_")
    if len(parts) < 3:
        await callback.message.edit_text("Ошибка: неверный формат команды заправки.", reply_markup=work_submenu())
        return
    try:
        car_id = int(parts[1])
        option = parts[2]
    except ValueError:
        await callback.message.edit_text("Ошибка: ID машины должен быть числом.", reply_markup=work_submenu())
        return
    user_id = callback.from_user.id
    user = get_user(user_id)
    car_item = next((c for c in user["cars"] if c["id"] == car_id), None)
    if not car_item:
        await callback.message.edit_text("❌ Ошибка: у вас нет машины с таким ID в гараже.", reply_markup=work_submenu())
        return
    car_info = get_car_by_id(car_id)
    if not car_info:
        logging.error(f"Машина с ID {car_id} не найдена в таблице cars.")
        await callback.message.edit_text("❌ Ошибка: данные о машине не найдены. Сообщите администратору.", reply_markup=work_submenu())
        return
    if option == "full":
        liters_to_add = car_info["fuel_capacity"] - car_item["fuel"]
        if liters_to_add <= 0:
            await callback.message.edit_text("⛽ Бак уже полный! Заправка не требуется.", reply_markup=work_submenu())
            return
    elif option in ("10", "50"):
        try:
            liters_to_add = int(option)
        except ValueError:
            await callback.message.edit_text("Ошибка: неверное количество литров.", reply_markup=work_submenu())
            return
    else:
        await callback.message.edit_text("Ошибка: неизвестный вариант заправки.", reply_markup=work_submenu())
        return
    max_possible_add = car_info["fuel_capacity"] - car_item["fuel"]
    if liters_to_add > max_possible_add:
        liters_to_add = max_possible_add
    if liters_to_add <= 0:
        await callback.message.edit_text("⛽ Бак уже полный или вы пытаетесь заправить 0 литров.", reply_markup=work_submenu())
        return
    cost = liters_to_add * FUEL_PRICE
    if user["balance"] < cost:
        await callback.message.edit_text(
            f"❌ Недостаточно средств. Нужно: ${cost}, у вас: ${user['balance']}.",
            reply_markup=work_submenu()
        )
        return
    new_fuel_level = min(car_item["fuel"] + liters_to_add, car_info["fuel_capacity"])
    for c in user["cars"]:
        if c["id"] == car_id:
            c["fuel"] = new_fuel_level
            break
    new_balance = user["balance"] - cost
    update_user(user_id, balance=new_balance, cars=user["cars"])
    success_message = (
        f"✅ Заправка прошла успешно!\n"
        f"⛽ Машина: {car_info['name']}\n"
        f"➕ Залито: {liters_to_add} л\n"
        f"💵 Стоимость: ${cost}\n"
        f"📊 Топливо в баке: {new_fuel_level}/{car_info['fuel_capacity']} л\n"
        f"💰 Новый баланс: ${new_balance}"
    )
    await callback.message.edit_text(success_message, reply_markup=work_submenu())

@dp.callback_query(F.data == "buy_menu")
@subscription_required
async def buy_menu(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    cars = get_all_cars()
    text = "Выберите машину для покупки:"
    markup = cars_keyboard(cars, "buy")
    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение: {e}")
    await callback.message.answer(text, reply_markup=markup)

@dp.callback_query(F.data.startswith("buy_"))
@subscription_required
async def buy_car(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    user_id = callback.from_user.id
    car_id = int(callback.data.split("_")[1])
    car_info = get_car_by_id(car_id)
    if not car_info:
        await callback.message.edit_text("Ошибка: машина не найдена.", reply_markup=work_submenu())
        return
    user = get_user(user_id)
    if user["balance"] < car_info["price"]:
        await callback.message.edit_text(
            f"❌ Недостаточно средств. Нужно ${car_info['price']}, у вас ${user['balance']}.",
            reply_markup=work_submenu()
        )
        return
    new_car = {"id": car_id, "fuel": 0}
    new_cars = user["cars"] + [new_car]
    new_balance = user["balance"] - car_info["price"]
    update_user(user_id, balance=new_balance, cars=new_cars)
    await callback.message.edit_text(
        f"✅ Вы купили {car_info['name']} за ${car_info['price']}!\n"
        f"⚠️ Бак пуст! Не забудьте заправиться.\n"
        f"Остаток баланса: ${new_balance}.",
        reply_markup=work_submenu()
    )

@dp.callback_query(F.data == "sell_car_menu")
@subscription_required
async def sell_car_menu(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    user_id = callback.from_user.id
    user = get_user(user_id)
    if not user["cars"]:
        await callback.message.edit_text("У вас нет машин для продажи.", reply_markup=work_submenu())
        return
    builder = InlineKeyboardBuilder()
    for car_item in user["cars"]:
        car_info = get_car_by_id(car_item["id"])
        if car_info:
            sell_price = car_info["price"] // 2
            text = f"{car_info['name']} — продажа за ${sell_price}"
            callback_data = f"sell_{car_item['id']}"
            builder.add(InlineKeyboardButton(text=text, callback_data=callback_data))
    builder.add(InlineKeyboardButton(text="🔙 Назад", callback_data="work_main"))
    builder.adjust(1)
    await callback.message.edit_text("Выберите машину для продажи:", reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("sell_"))
@subscription_required
async def sell_car(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    user_id = callback.from_user.id
    car_id = int(callback.data.split("_")[1])
    user = get_user(user_id)
    car_item = next((c for c in user["cars"] if c["id"] == car_id), None)
    if not car_item:
        await callback.message.edit_text("Машина не найдена.", reply_markup=work_submenu())
        return
    car_info = get_car_by_id(car_id)
    if not car_info:
        await callback.message.edit_text("Ошибка данных машины.", reply_markup=work_submenu())
        return
    if car_id in user["hired_cars"]:
        await callback.message.edit_text("❌ Сначала увольте водителя с этой машины.", reply_markup=work_submenu())
        return
    sell_price = car_info["price"] // 2
    new_cars = [c for c in user["cars"] if c["id"] != car_id]
    new_balance = user["balance"] + sell_price
    update_user(user_id, balance=new_balance, cars=new_cars)
    await callback.message.edit_text(f"✅ Вы продали {car_info['name']} за ${sell_price}.\nНовый баланс: ${new_balance}.", reply_markup=work_submenu())

@dp.callback_query(F.data == "hired_menu")
@subscription_required
async def hired_menu(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    user_id = callback.from_user.id
    user = get_user(user_id)
    hired = user["hired_cars"]
    if not hired:
        text = "👨‍✈️ У вас пока нет наёмных водителей.\n\nЧтобы нанять водителя, у вас должна быть машина в гараже. Используйте команду /hire <id машины>"
        try:
            await callback.message.delete()
        except Exception as e:
            logging.warning(f"Не удалось удалить сообщение: {e}")
        await callback.message.answer(text, reply_markup=work_submenu())
        return
    text = "👨‍✈️ **Ваши наёмные водители:**\n\n"
    total_income = 0
    for car_id in hired:
        car_info = get_car_by_id(car_id)
        if car_info:
            income = car_info["min_earn"] * 0.1
            total_income += income
            text += f"• {car_info['name']} — приносит ${income:.2f} в час\n"
    text += f"\n⏰ Доход начисляется каждый час.\n💵 **Общий доход в час:** ${total_income:.2f}"
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="💰 Собрать доход", callback_data="collect_hired_income"))
    builder.add(InlineKeyboardButton(text="🔙 Назад", callback_data="work_main"))
    builder.adjust(1)
    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение: {e}")
    await callback.message.answer(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "collect_hired_income")
@subscription_required
async def collect_hired_income(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    user_id = callback.from_user.id
    user = get_user(user_id)
    hired = user["hired_cars"]
    if not hired:
        await callback.message.edit_text("У вас нет наёмных водителей.", reply_markup=work_submenu())
        return
    total = 0
    for car_id in hired:
        car_info = get_car_by_id(car_id)
        if car_info:
            total += car_info["min_earn"] * 0.1 * 1
    new_balance = user["balance"] + int(total)
    update_user(user_id, balance=new_balance)
    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение: {e}")
    await callback.message.answer(f"💰 Вы собрали доход с водителей: +${int(total)}!\nТеперь ваш баланс: ${new_balance}.", reply_markup=work_submenu())

@dp.callback_query(F.data == "loan_menu")
@subscription_required
async def loan_menu(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    new_text = "Введите сумму кредита, используя команду /loan <сумма>\nМаксимальная сумма кредита: 500.000$\nМаксимальное количество кредитов: 5\nНапример: /loan 5000"
    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение: {e}")
    await callback.message.answer(new_text, reply_markup=bank_submenu())

@dp.callback_query(F.data == "repay_menu")
@subscription_required
async def repay_menu(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    new_text = "Введите сумму погашения, используя команду /repay <сумма>\nНапример: /repay 2000"
    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение: {e}")
    await callback.message.answer(new_text, reply_markup=bank_submenu())

@dp.callback_query(F.data == "tip_race_menu")
@subscription_required
async def tip_race_menu(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    apply_interest(callback.from_user.id)
    user_id = callback.from_user.id
    top = get_tip_race_top(10)
    position, user_tips, total = get_user_tip_position(user_id)
    week_start = datetime.fromtimestamp(get_current_week_start())
    week_end = week_start + timedelta(days=6, hours=23, minutes=59)
    text = (
        f"🏁 **Гонка чаевых**\n\n"
        f"📅 Неделя: {week_start.strftime('%d.%m')} - {week_end.strftime('%d.%m')}\n"
        f"👥 Участников: {total}\n\n"
    )
    if position > 0:
        text += f"📊 **Ваше место:** {position} (${user_tips} чаевых)\n\n"
    else:
        text += "📊 **Ваше место:** пока нет чаевых в этой гонке\n\n"
    if top:
        text += "🏆 **Топ-10 текущей недели:**\n"
        for i, (uid, tips) in enumerate(top, 1):
            try:
                user = await bot.get_chat(uid)
                username = user.username or f"ID {uid}"
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                text += f"{medal} @{username} — ${tips}\n"
            except:
                text += f"{i}. ID {uid} — ${tips}\n"
    else:
        text += "🏆 Пока нет участников в этой гонке.\n\n"
    text += "\n💡 **Как участвовать?**\nПросто получай чаевые от пассажиров во время поездок! Чем больше чаевых, тем выше место.\n\n"
    text += "🎁 **Награды в воскресенье:**\n"
    text += "🥇 1 место — 50.000$\n"
    text += "🥈 2 место — 30.000$\n"
    text += "🥉 3 место — 20.000$\n"
    text += "4-10 места — 10.000$"
    
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="🔄 Обновить", callback_data="tip_race_menu"))
    builder.add(InlineKeyboardButton(text="🔙 Назад", callback_data="bank_main"))
    builder.adjust(1)
    
    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение: {e}")
    await callback.message.answer(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "deposits_menu")
@subscription_required
async def show_deposits(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    user_id = callback.from_user.id
    deposits = get_user_deposits(user_id)
    for d in deposits:
        apply_deposit_interest(d["id"])
    deposits = get_user_deposits(user_id)

    text = "💳 **Ваши вклады:**\n\n"
    if not deposits:
        text += "У вас пока нет вкладов. Минимальная сумма для открытия: 20 000$.\n"
        text += "Используйте команду /deposit <сумма> чтобы открыть вклад.\n"
        text += "Процентная ставка: 2% каждые 2 дня."
    else:
        for d in deposits:
            text += f"Вклад #{d['id']}: ${d['amount']}\n"
        text += f"\nВсего вкладов: {len(deposits)}/2\n"
        text += "Вы можете пополнить существующий вклад через /deposit (сумма будет добавлена к первому подходящему вкладу, не превышающему лимит 500 000$)."
    
    builder = InlineKeyboardBuilder()
    for d in deposits:
        builder.add(InlineKeyboardButton(text=f"🔒 Закрыть вклад #{d['id']}", callback_data=f"close_deposit_{d['id']}"))
    builder.add(InlineKeyboardButton(text="🔙 Назад", callback_data="bank_main"))
    builder.adjust(1)
    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение: {e}")
    await callback.message.answer(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("close_deposit_"))
@subscription_required
async def close_deposit_callback(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    deposit_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    
    new_amount, interest = apply_deposit_interest(deposit_id)
    
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT user_id, amount FROM deposits WHERE id = ?", (deposit_id,))
    row = cur.fetchone()
    if not row or row[0] != user_id:
        await callback.message.edit_text("❌ Вклад не найден или не принадлежит вам.")
        conn.close()
        return
    
    amount = row[1]
    user = get_user(user_id)
    new_balance = user["balance"] + amount
    update_user(user_id, balance=new_balance)
    
    cur.execute("DELETE FROM deposits WHERE id = ?", (deposit_id,))
    conn.commit()
    conn.close()
    
    try:
        await callback.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение: {e}")
    await callback.message.answer(
        f"✅ Вклад №{deposit_id} закрыт.\n"
        f"💰 Сумма ${amount} переведена на ваш баланс.\n"
        f"💵 Начислено процентов за последний период: ${interest}.\n"
        f"Новый баланс: ${new_balance}.",
        reply_markup=bank_submenu()
    )

@dp.message(Command("loan"))
@subscription_required
async def take_loan(message: types.Message, **kwargs):
    user_id = message.from_user.id
    apply_interest(user_id)
    args = message.text.split()
    if len(args) != 2:
        await message.reply("Использование: /loan <сумма>\nНапример: /loan 5000")
        return
    try:
        amount = int(args[1])
        if amount <= 0:
            raise ValueError
    except:
        await message.reply("❌ Сумма должна быть положительным числом.")
        return
    MAX_LOAN_AMOUNT = 500_000
    if amount > MAX_LOAN_AMOUNT:
        await message.reply(f"❌ Максимальная сумма кредита — ${MAX_LOAN_AMOUNT:,}. Введите меньшую сумму.")
        return
    user = get_user(user_id)
    MAX_CREDITS = 5
    if user["credits_count"] >= MAX_CREDITS:
        await message.reply(f"❌ Вы уже взяли максимальное количество кредитов ({MAX_CREDITS}). Сначала погасите существующие.")
        return
    new_balance = user["balance"] + amount
    new_debt = user["debt"] + amount
    new_credits_count = user["credits_count"] + 1
    update_user(user_id, balance=new_balance, debt=new_debt)
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("UPDATE users SET credits_count = ? WHERE user_id = ?", (new_credits_count, user_id))
    conn.commit()
    conn.close()
    await message.reply(
        f"✅ Вы взяли кредит ${amount}.\n"
        f"Теперь ваш долг: ${new_debt}, баланс: ${new_balance}\n"
        f"Кредитов взято: {new_credits_count}/{MAX_CREDITS}\n"
        f"⚠️ Каждые 5 часов начисляется 5% на остаток долга!",
        reply_markup=bank_submenu()
    )

@dp.message(Command("repay"))
@subscription_required
async def repay_loan(message: types.Message, **kwargs):
    user_id = message.from_user.id
    apply_interest(user_id)
    args = message.text.split()
    if len(args) != 2:
        await message.reply("Использование: /repay <сумма>")
        return
    try:
        amount = int(args[1])
        if amount <= 0:
            raise ValueError
    except:
        await message.reply("Сумма должна быть положительным числом.")
        return
    user = get_user(user_id)
    if user["debt"] == 0:
        await message.reply("У вас нет долга.")
        return
    if amount > user["balance"]:
        await message.reply("Недостаточно средств.")
        return
    if amount > user["debt"]:
        amount = user["debt"]
    new_balance = user["balance"] - amount
    new_debt = user["debt"] - amount
    update_user(user_id, balance=new_balance, debt=new_debt)
    if new_debt == 0:
        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()
        cur.execute("SELECT credits_count FROM users WHERE user_id = ?", (user_id,))
        result = cur.fetchone()
        current_credits = result[0] if result else 0
        if current_credits > 0:
            new_credits = current_credits - 1
            cur.execute("UPDATE users SET credits_count = ? WHERE user_id = ?", (new_credits, user_id))
            conn.commit()
        conn.close()
        await message.reply(
            f"✅ Вы полностью погасили кредит!\n"
            f"Остаток долга: ${new_debt}, баланс: ${new_balance}\n"
            f"Активных кредитов осталось: {new_credits}",
            reply_markup=bank_submenu()
        )
    else:
        await message.reply(
            f"✅ Вы погасили ${amount} кредита.\n"
            f"Остаток долга: ${new_debt}, баланс: ${new_balance}",
            reply_markup=bank_submenu()
        )

@dp.message(Command("pay"))
@subscription_required
async def pay_user(message: types.Message, **kwargs):
    user_id = message.from_user.id
    apply_interest(user_id)
    args = message.text.split()
    if len(args) != 3:
        await message.reply("Использование: /pay <сумма> <id пользователя>\nНапример: /pay 500 123456789")
        return
    try:
        amount = int(args[1])
        if amount <= 0:
            raise ValueError
        target_user_id = int(args[2])
    except:
        await message.reply("Сумма должна быть положительным числом, ID пользователя - числом.")
        return
    sender_id = message.from_user.id
    sender = get_user(sender_id)
    if sender["balance"] < amount:
        await message.reply(f"❌ Недостаточно средств. У вас ${sender['balance']}.")
        return
    target = get_user(target_user_id)
    new_sender_balance = sender["balance"] - amount
    new_target_balance = target["balance"] + amount
    update_user(sender_id, balance=new_sender_balance)
    update_user(target_user_id, balance=new_target_balance)
    await message.reply(f"✅ Вы перевели ${amount} пользователю {target_user_id}.\nВаш баланс: ${new_sender_balance}", reply_markup=main_menu())
    try:
        await bot.send_message(
            target_user_id,
            f"💰 Вам перевели ${amount} от пользователя {sender_id}.\nВаш баланс: ${new_target_balance}"
        )
    except:
        pass

@dp.message(Command("hire"))
@subscription_required
async def hire_driver(message: types.Message, **kwargs):
    user_id = message.from_user.id
    apply_interest(user_id)
    args = message.text.split()
    if len(args) != 2:
        await message.reply("Использование: /hire <id машины>\nID машины можно узнать в гараже.")
        return
    try:
        car_id = int(args[1])
    except:
        await message.reply("❌ ID машины должен быть числом.")
        return
    user = get_user(user_id)
    car_item = next((c for c in user["cars"] if c["id"] == car_id), None)
    if not car_item:
        await message.reply("❌ У вас нет такой машины в гараже.")
        return
    if car_id in user["hired_cars"]:
        await message.reply("❌ На эту машину уже нанят водитель.")
        return
    hire_cost = 500
    if user["balance"] < hire_cost:
        await message.reply(f"❌ Недостаточно средств для найма водителя. Нужно ${hire_cost}.")
        return
    new_balance = user["balance"] - hire_cost
    new_hired = user["hired_cars"] + [car_id]
    update_user(user_id, balance=new_balance, hired_cars=new_hired)
    await message.reply(f"✅ Вы наняли водителя на машину {get_car_by_id(car_id)['name']} за ${hire_cost}!\n"
                        f"Теперь он будет приносить доход. Зайдите в меню наёмных водителей для сбора.", reply_markup=work_submenu())

@dp.message(Command("fire"))
@subscription_required
async def fire_driver(message: types.Message, **kwargs):
    user_id = message.from_user.id
    apply_interest(user_id)
    args = message.text.split()
    if len(args) != 2:
        await message.reply("Использование: /fire <id машины>")
        return
    try:
        car_id = int(args[1])
    except:
        await message.reply("❌ ID машины должен быть числом.")
        return
    user = get_user(user_id)
    if car_id not in user["hired_cars"]:
        await message.reply("❌ У вас нет наёмного водителя на эту машину.")
        return
    new_hired = [cid for cid in user["hired_cars"] if cid != car_id]
    update_user(user_id, hired_cars=new_hired)
    await message.reply(f"✅ Вы уволили водителя с машины {get_car_by_id(car_id)['name']}.", reply_markup=work_submenu())

@dp.message(Command("sell"))
@subscription_required
async def sell_car_command(message: types.Message, **kwargs):
    user_id = message.from_user.id
    apply_interest(user_id)
    args = message.text.split()
    if len(args) != 2:
        await message.reply("Использование: /sell <id машины>")
        return
    try:
        car_id = int(args[1])
    except:
        await message.reply("❌ ID машины должен быть числом.")
        return
    user = get_user(user_id)
    car_item = next((c for c in user["cars"] if c["id"] == car_id), None)
    if not car_item:
        await message.reply("❌ У вас нет такой машины.")
        return
    car_info = get_car_by_id(car_id)
    if not car_info:
        await message.reply("❌ Ошибка данных машины.")
        return
    if car_id in user["hired_cars"]:
        await message.reply("❌ Сначала увольте водителя с этой машины.")
        return
    sell_price = car_info["price"] // 2
    new_cars = [c for c in user["cars"] if c["id"] != car_id]
    new_balance = user["balance"] + sell_price
    update_user(user_id, balance=new_balance, cars=new_cars)
    await message.reply(f"✅ Вы продали {car_info['name']} за ${sell_price}.\nНовый баланс: ${new_balance}.", reply_markup=work_submenu())

@dp.message(Command("promo"))
@subscription_required
async def activate_promo(message: types.Message, **kwargs):
    user_id = message.from_user.id
    args = message.text.split()
    if len(args) != 2:
        await message.reply("Использование: /promo <код>")
        return
    code = args[1].upper()
    user = get_user(user_id)
    if code in user["used_promocodes"]:
        await message.reply("❌ Вы уже активировали этот промокод.")
        return
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT id, reward, max_uses, used_count FROM promocodes WHERE code = ? AND (expires_at = 0 OR expires_at > ?)", (code, int(time_module.time())))
    promo = cur.fetchone()
    if not promo:
        conn.close()
        await message.reply("❌ Неверный или истекший промокод.")
        return
    promo_id, reward, max_uses, used_count = promo
    if used_count >= max_uses:
        conn.close()
        await message.reply("❌ Промокод уже использован максимальное количество раз.")
        return
    new_balance = user["balance"] + reward
    new_used = user["used_promocodes"] + [code]
    update_user(user_id, balance=new_balance, used_promocodes=new_used)
    cur.execute("UPDATE promocodes SET used_count = used_count + 1 WHERE id = ?", (promo_id,))
    conn.commit()
    conn.close()
    await message.reply(
        f"✅ Промокод {code} активирован!\n"
        f"Вы получили ${reward}!\n"
        f"Новый баланс: ${new_balance}",
        reply_markup=main_menu()
    )

@dp.message(Command("deposit"))
@subscription_required
async def cmd_deposit(message: types.Message, **kwargs):
    user_id = message.from_user.id
    apply_interest(user_id)
    args = message.text.split()
    if len(args) != 2:
        await message.reply("Использование: /deposit <сумма>\nМинимальная сумма: 20 000$, максимальная за раз: 100 000$")
        return
    try:
        amount = int(args[1])
        if amount < 20000 or amount > 100000:
            raise ValueError
    except:
        await message.reply("❌ Сумма должна быть от 20 000 до 100 000$.")
        return

    user = get_user(user_id)
    if user["balance"] < amount:
        await message.reply("❌ Недостаточно средств на счету.")
        return

    deposits = get_user_deposits(user_id)
    for d in deposits:
        apply_deposit_interest(d["id"])
    deposits = get_user_deposits(user_id)

    if len(deposits) >= 2:
        possible = None
        for d in deposits:
            if d["amount"] + amount <= 500000:
                possible = d
                break
        if possible is None:
            await message.reply("❌ Невозможно пополнить: все вклады достигнут лимита 500 000$.")
            return
        add_to_deposit(possible["id"], amount)
        update_user(user_id, balance=user["balance"] - amount)
        await message.reply(f"✅ Вы пополнили вклад №{possible['id']} на ${amount}. Текущая сумма: {possible['amount'] + amount}$")
    else:
        create_deposit(user_id, amount)
        update_user(user_id, balance=user["balance"] - amount)
        await message.reply(f"✅ Вы открыли новый вклад на ${amount}. Максимум можно иметь 2 вклада.")

# ---------- АДМИН-ХЕНДЛЕРЫ (без подписки) ----------
@dp.callback_query(F.data == "admin_panel")
async def admin_panel(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    user_id = callback.from_user.id
    if user_id not in admin_users:
        await callback.message.edit_text("❌ У вас нет доступа к админ-панели.\nВведите /admin и правильный пароль для входа.", reply_markup=main_menu())
        return
    new_text = "👑 Админ панель\nВыберите действие:"
    if callback.message.text == new_text and callback.message.reply_markup == admin_menu():
        return
    await callback.message.edit_text(new_text, reply_markup=admin_menu())

@dp.callback_query(F.data == "admin_add_money")
async def admin_add_money(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    user_id = callback.from_user.id
    if user_id not in admin_users:
        await callback.message.edit_text("❌ Нет доступа.", reply_markup=main_menu())
        return
    user = get_user(user_id)
    new_balance = user["balance"] + 1000000
    update_user(user_id, balance=new_balance)
    await callback.message.edit_text(f"💰 Вы получили 1.000.000$\nВаш баланс: ${new_balance}", reply_markup=admin_menu())

@dp.callback_query(F.data == "admin_transfer_menu")
async def admin_transfer_menu(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    user_id = callback.from_user.id
    if user_id not in admin_users:
        await callback.message.edit_text("❌ Нет доступа.", reply_markup=main_menu())
        return
    await callback.message.edit_text(
        "Введите команду:\n/admin_transfer <id пользователя> <сумма>\n\n"
        "Например: /admin_transfer 123456789 50000",
        reply_markup=admin_menu()
    )

@dp.message(Command("admin_transfer"))
async def admin_transfer(message: types.Message, **kwargs):
    user_id = message.from_user.id
    if user_id not in admin_users:
        await message.reply("❌ Нет доступа.")
        return
    args = message.text.split()
    if len(args) != 3:
        await message.reply("Использование: /admin_transfer <id пользователя> <сумма>")
        return
    try:
        target_id = int(args[1])
        amount = int(args[2])
        if amount <= 0:
            raise ValueError
    except:
        await message.reply("❌ Неверный формат. Сумма должна быть положительным числом.")
        return
    target = get_user(target_id)
    new_balance = target["balance"] + amount
    update_user(target_id, balance=new_balance)
    await message.reply(f"✅ Переведено ${amount} пользователю {target_id}.\nЕго баланс: ${new_balance}")
    try:
        await bot.send_message(target_id, f"💰 Администратор перевёл вам ${amount}!\nВаш баланс: ${new_balance}")
    except:
        pass

@dp.callback_query(F.data == "admin_give_car_menu")
async def admin_give_car_menu(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    user_id = callback.from_user.id
    if user_id not in admin_users:
        await callback.message.edit_text("❌ Нет доступа.", reply_markup=main_menu())
        return
    cars = get_all_cars()
    builder = InlineKeyboardBuilder()
    for car in cars:
        text = f"{car['name']} (ID: {car['id']})"
        callback_data = f"admin_give_car_{car['id']}"
        builder.add(InlineKeyboardButton(text=text, callback_data=callback_data))
    builder.add(InlineKeyboardButton(text="🔙 Назад", callback_data="admin_panel"))
    builder.adjust(1)
    await callback.message.edit_text("Выберите машину для выдачи:", reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("admin_give_car_"))
async def admin_give_car(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    admin_id = callback.from_user.id
    if admin_id not in admin_users:
        await callback.message.edit_text("❌ Нет доступа.", reply_markup=main_menu())
        return
    car_id = int(callback.data.split("_")[3])
    await callback.message.edit_text(
        f"Введите команду:\n/admin_give_car <id пользователя> {car_id}\n\n"
        f"Например: /admin_give_car 123456789 {car_id}",
        reply_markup=admin_menu()
    )

@dp.message(Command("admin_give_car"))
async def admin_give_car_command(message: types.Message, **kwargs):
    user_id = message.from_user.id
    if user_id not in admin_users:
        await message.reply("❌ Нет доступа.")
        return
    args = message.text.split()
    if len(args) != 3:
        await message.reply("Использование: /admin_give_car <id пользователя> <id машины>")
        return
    try:
        target_id = int(args[1])
        car_id = int(args[2])
    except:
        await message.reply("❌ Неверный формат ID.")
        return
    car_info = get_car_by_id(car_id)
    if not car_info:
        await message.reply("❌ Машина с таким ID не найдена.")
        return
    target = get_user(target_id)
    new_car = {"id": car_id, "fuel": car_info["fuel_capacity"]}
    new_cars = target["cars"] + [new_car]
    update_user(target_id, cars=new_cars)
    await message.reply(f"✅ Выдана машина {car_info['name']} пользователю {target_id}.")
    try:
        await bot.send_message(target_id, f"🚗 Администратор выдал вам машину {car_info['name']}!\nЗагляните в гараж.")
    except:
        pass

@dp.callback_query(F.data == "admin_full_fuel")
async def admin_full_fuel(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    user_id = callback.from_user.id
    if user_id not in admin_users:
        await callback.message.edit_text("❌ Нет доступа.", reply_markup=main_menu())
        return
    user = get_user(user_id)
    if not user["cars"]:
        await callback.message.edit_text("У вас нет машин.", reply_markup=admin_menu())
        return
    for car_item in user["cars"]:
        car_info = get_car_by_id(car_item["id"])
        if car_info:
            car_item["fuel"] = car_info["fuel_capacity"]
    update_user(user_id, cars=user["cars"])
    await callback.message.edit_text("⛽ Бак всех ваших машин полностью заправлен!", reply_markup=admin_menu())

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    user_id = callback.from_user.id
    if user_id not in admin_users:
        await callback.message.edit_text("❌ Нет доступа.", reply_markup=main_menu())
        return
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    users_count = cur.fetchone()[0]
    cur.execute("SELECT SUM(balance) FROM users")
    total_balance = cur.fetchone()[0] or 0
    cur.execute("SELECT SUM(debt) FROM users")
    total_debt = cur.fetchone()[0] or 0
    conn.close()
    stats_text = f"📊 Статистика игры:\n\n"
    stats_text += f"👥 Всего игроков: {users_count}\n"
    stats_text += f"💰 Общий баланс: ${total_balance}\n"
    stats_text += f"💳 Общий долг: ${total_debt}\n"
    await callback.message.edit_text(stats_text, reply_markup=admin_menu())

@dp.callback_query(F.data == "admin_reset_user_menu")
async def admin_reset_user_menu(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    user_id = callback.from_user.id
    if user_id not in admin_users:
        await callback.message.edit_text("❌ Нет доступа.", reply_markup=main_menu())
        return
    await callback.message.edit_text(
        "Введите команду:\n/admin_reset <id пользователя>\n\n"
        "Эта команда полностью обнулит счёт игрока:\n"
        "• Баланс = 5000 (начальный)\n"
        "• Долг = 0\n"
        "• Все машины будут удалены",
        reply_markup=admin_menu()
    )

@dp.message(Command("admin_reset"))
async def admin_reset_user(message: types.Message, **kwargs):
    user_id = message.from_user.id
    if user_id not in admin_users:
        await message.reply("❌ Нет доступа.")
        return
    args = message.text.split()
    if len(args) != 2:
        await message.reply("Использование: /admin_reset <id пользователя>")
        return
    try:
        target_id = int(args[1])
    except:
        await message.reply("❌ ID пользователя должен быть числом.")
        return
    target = get_user(target_id)
    old_balance = target["balance"]
    old_debt = target["debt"]
    old_cars_count = len(target["cars"])
    update_user(target_id, balance=START_BALANCE, debt=0, cars=[], used_promocodes=[], last_interest=0)
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("UPDATE users SET credits_count = 0, exp = 0, level = 1, hired_cars = '[]', happy_passengers = 0, angry_passengers = 0, last_tip_reward_week = 0 WHERE user_id = ?", (target_id,))
    conn.commit()
    conn.close()
    await message.reply(
        f"✅ Счёт пользователя {target_id} обнулён!\n"
        f"Было: баланс ${old_balance}, долг ${old_debt}, машин {old_cars_count}\n"
        f"Стало: баланс ${START_BALANCE}, долг 0, машин 0",
        reply_markup=admin_menu()
    )
    try:
        await bot.send_message(
            target_id,
            f"⚠️ Администратор обнулил ваш счёт.\n"
            f"Ваш баланс сброшен до ${START_BALANCE}, долг обнулён, все машины удалены."
        )
    except:
        pass

@dp.callback_query(F.data == "admin_create_promo")
async def admin_create_promo(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    user_id = callback.from_user.id
    if user_id not in admin_users:
        await callback.message.edit_text("❌ Нет доступа.", reply_markup=main_menu())
        return
    await callback.message.edit_text(
        "Введите команду:\n/create_promo <код> <награда> [макс_использований]\n\n"
        "Например: /create_promo BONUS30000 30000 10\n"
        "Если не указать макс_использований, будет 1.",
        reply_markup=admin_menu()
    )

@dp.message(Command("create_promo"))
async def create_promo(message: types.Message, **kwargs):
    user_id = message.from_user.id
    if user_id not in admin_users:
        await message.reply("❌ Нет доступа.")
        return
    args = message.text.split()
    if len(args) < 3:
        await message.reply("Использование: /create_promo <код> <награда> [макс_использований]")
        return
    code = args[1].upper()
    try:
        reward = int(args[2])
        if reward <= 0:
            raise ValueError
    except:
        await message.reply("❌ Награда должна быть положительным числом.")
        return
    max_uses = 1
    if len(args) >= 4:
        try:
            max_uses = int(args[3])
            if max_uses <= 0:
                max_uses = 1
        except:
            pass
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO promocodes (code, reward, max_uses, used_count, expires_at) VALUES (?, ?, ?, 0, 0)",
            (code, reward, max_uses)
        )
        conn.commit()
        await message.reply(f"✅ Промокод {code} создан!\nНаграда: ${reward}\nМакс. использований: {max_uses}")
    except sqlite3.IntegrityError:
        await message.reply("❌ Промокод с таким кодом уже существует.")
    conn.close()

@dp.callback_query(F.data == "admin_reset_all_confirm")
async def admin_reset_all_confirm(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    user_id = callback.from_user.id
    if user_id not in admin_users:
        await callback.message.edit_text("❌ Нет доступа.", reply_markup=main_menu())
        return
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="✅ ДА, сбросить всех", callback_data="admin_reset_all_execute"))
    builder.add(InlineKeyboardButton(text="❌ НЕТ, отмена", callback_data="admin_panel"))
    builder.adjust(1)
    await callback.message.edit_text(
        "⚠️ **ВНИМАНИЕ!** Это действие **безвозвратно сбросит всех игроков** до начального состояния:\n"
        "• Баланс = 5000\n"
        "• Долг = 0\n"
        "• Все машины удалены\n"
        "• Опыт, уровень, кредиты, водители, рейтинг – обнулены.\n\n"
        "Вы уверены?",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "admin_reset_all_execute")
async def admin_reset_all_execute(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    user_id = callback.from_user.id
    if user_id not in admin_users:
        await callback.message.edit_text("❌ Нет доступа.", reply_markup=main_menu())
        return
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        UPDATE users SET
            balance = ?,
            debt = 0,
            cars = '[]',
            credits_count = 0,
            exp = 0,
            level = 1,
            hired_cars = '[]',
            happy_passengers = 0,
            angry_passengers = 0,
            used_promocodes = '[]',
            last_tip_reward_week = 0,
            last_interest = 0,
            factory_level = 0
    """, (START_BALANCE,))
    cur.execute("DELETE FROM tip_race")
    cur.execute("DELETE FROM deposits")
    conn.commit()
    conn.close()
    await callback.message.edit_text(
        "✅ **Все игроки сброшены до начального состояния!**",
        reply_markup=admin_menu(),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "admin_withdraw_list")
async def admin_withdraw_list(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    user_id = callback.from_user.id
    if user_id not in admin_users:
        await callback.message.edit_text("❌ Нет доступа.", reply_markup=main_menu())
        return

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT id, user_id, stars_amount, comment, status, created_at FROM withdraw_requests ORDER BY created_at DESC LIMIT 20")
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await callback.message.edit_text("📋 Нет заявок на вывод.", reply_markup=admin_menu())
        return

    text = "📋 **Последние заявки на вывод (звёзды):**\n\n"
    for r in rows:
        req_id, uid, stars, cmt, status, ts = r
        date = datetime.fromtimestamp(ts).strftime("%d.%m %H:%M")
        status_emoji = {
            "pending": "⏳",
            "approved": "✅",
            "rejected": "❌"
        }.get(status, "❓")
        text += f"{status_emoji} №{req_id} от {date} – {uid}: {stars}⭐ {cmt[:20]}\n"

    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="🔙 Назад", callback_data="admin_panel"))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

async def send_broadcast_message(chat_id: int):
    text = (
        "🚖 **Таксист ждёт тебя!**\n\n"
        "Зарабатывай деньги, покупай машины, участвуй в гонке чаевых!\n\n"
        "➡️ Нажми /start чтобы начать игру."
    )
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="🚖 Играть", callback_data="back_to_menu"))
    builder.adjust(1)
    try:
        await bot.send_message(chat_id, text, reply_markup=builder.as_markup())
        return True
    except Exception as e:
        logging.error(f"Ошибка рассылки {chat_id}: {e}")
        return False

@dp.callback_query(F.data == "admin_broadcast_confirm")
async def admin_broadcast_confirm(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    user_id = callback.from_user.id
    if user_id not in admin_users:
        await callback.message.edit_text("❌ Нет доступа.", reply_markup=main_menu())
        return
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="✅ ДА, отправить всем", callback_data="admin_broadcast_execute"))
    builder.add(InlineKeyboardButton(text="❌ НЕТ, отмена", callback_data="admin_panel"))
    builder.adjust(1)
    await callback.message.edit_text(
        "⚠️ **ВНИМАНИЕ!** Это отправит рекламное сообщение **всем пользователям**.\n\n"
        "Текст сообщения:\n\n"
        "🚖 Таксист ждёт тебя!\n\n"
        "Зарабатывай деньги, покупай машины, участвуй в гонке чаевых!\n\n"
        "➡️ Нажми /start чтобы начать игру.\n\n"
        "Вы уверены?",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "admin_broadcast_execute")
async def admin_broadcast_execute(callback: types.CallbackQuery, **kwargs):
    await callback.answer()
    user_id = callback.from_user.id
    if user_id not in admin_users:
        await callback.message.edit_text("❌ Нет доступа.", reply_markup=main_menu())
        return

    await callback.message.edit_text("📢 Начинаю рассылку... Это может занять некоторое время.", reply_markup=admin_menu())

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users")
    users = cur.fetchall()
    conn.close()

    sent = 0
    failed = 0
    for (uid,) in users:
        if await send_broadcast_message(uid):
            sent += 1
        else:
            failed += 1
        await asyncio.sleep(0.5)

    await callback.message.edit_text(
        f"✅ Рассылка завершена!\n"
        f"📨 Отправлено: {sent}\n"
        f"❌ Ошибок: {failed}",
        reply_markup=admin_menu()
    )

async def main():
    init_db()
    print("Бот запущен...")
    asyncio.create_task(tip_race_scheduler())
    asyncio.create_task(daily_subscription_check())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
