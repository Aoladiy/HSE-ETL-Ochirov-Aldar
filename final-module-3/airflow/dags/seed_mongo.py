"""
DAG: seed_mongo
────────────────────────────────────────────────────────────────
Генерация тестовых данных в MongoDB (база ecommerce).

Создаёт 5 коллекций:
  1. user_sessions      — 2000 сессий (+50 дублей)
  2. event_logs          — 5000 событий (+100 дублей)
  3. support_tickets     — 600 обращений
  4. user_recommendations — 200 записей
  5. moderation_queue    — 800 отзывов

Запускается вручную один раз перед репликацией.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

MONGO_URI = "mongodb://etl-mongo:27017"
MONGO_DB = "ecommerce"

NUM_USERS = 200
NUM_SESSIONS = 2000
NUM_EVENTS = 5000
NUM_TICKETS = 600
NUM_REVIEWS = 800

PAGES = [
    "/home", "/products", "/products/42", "/products/101",
    "/products/205", "/cart", "/checkout", "/profile",
    "/about", "/faq", "/support",
]
DEVICES = ["mobile", "desktop", "tablet"]
ACTIONS = [
    "login", "view_product", "add_to_cart", "remove_from_cart",
    "search", "logout", "apply_coupon", "checkout",
]
EVENT_TYPES = [
    "click", "page_view", "scroll", "form_submit",
    "error", "api_call", "purchase",
]
STATUSES = ["open", "in_progress", "resolved", "closed"]
ISSUE_TYPES = ["payment", "delivery", "refund", "account", "product_quality", "other"]
MODERATION_STATUSES = ["pending", "approved", "rejected"]
FLAGS = [
    "contains_images", "suspicious_language", "short_text",
    "verified_purchase", "first_review",
]
USER_MESSAGES = [
    "Не могу оплатить заказ.",
    "Товар пришёл повреждённым.",
    "Когда будет возврат?",
    "Проблема с аккаунтом.",
    "Доставка задерживается.",
    "Не могу войти в аккаунт.",
]
SUPPORT_MESSAGES = [
    "Пожалуйста, уточните номер заказа.",
    "Мы передали ваш запрос в отдел.",
    "Возврат будет обработан в течение 3 дней.",
    "Попробуйте сбросить пароль.",
    "Приносим извинения за неудобства.",
]
REVIEW_TEXTS = [
    "Отличный товар, работает как нужно!",
    "Не стоит своих денег.",
    "Доставка быстрая, товар качественный.",
    "Пришёл с дефектом, жду замену.",
    "Хороший товар за свою цену.",
    "Полностью доволен покупкой!",
    "Качество среднее, ожидал лучшего.",
    "Рекомендую всем!",
    "Упаковка была повреждена.",
    "Всё отлично, спасибо!",
    "Не соответствует описанию.",
    "Буду заказывать ещё.",
]


def _rand_date(start_str: str, end_str: str) -> datetime:
    s = datetime.fromisoformat(start_str)
    e = datetime.fromisoformat(end_str)
    delta = (e - s).total_seconds()
    return s + timedelta(seconds=random.random() * delta)


def _get_mongo_db():
    from pymongo import MongoClient
    client = MongoClient(MONGO_URI)
    return client, client[MONGO_DB]


# ─── Генераторы ───────────────────────────────────────────────

def _generate_user_ids() -> list[str]:
    return [f"user_{i + 1:04d}" for i in range(NUM_USERS)]


def _generate_product_ids() -> list[str]:
    return [f"prod_{i + 1:03d}" for i in range(50)]


def seed_user_sessions() -> None:
    """Генерация коллекции user_sessions."""
    client, db = _get_mongo_db()
    user_ids = _generate_user_ids()

    db.user_sessions.drop()

    docs = []
    for i in range(NUM_SESSIONS):
        start = _rand_date("2024-01-01", "2024-06-30")
        end = start + timedelta(minutes=random.randint(1, 120))
        docs.append({
            "session_id": f"sess_{i + 1:05d}",
            "user_id": random.choice(user_ids),
            "start_time": start.isoformat() + "Z",
            "end_time": end.isoformat() + "Z",
            "pages_visited": [random.choice(PAGES) for _ in range(random.randint(1, 8))],
            "device": random.choice(DEVICES),
            "actions": [random.choice(ACTIONS) for _ in range(random.randint(1, 6))],
        })

    db.user_sessions.insert_many(docs)
    # Добавляем дубликаты для демонстрации дедупликации
    # Убираем _id, чтобы MongoDB назначила новые
    dupes = [{k: v for k, v in d.items() if k != "_id"} for d in docs[:50]]
    db.user_sessions.insert_many(dupes)

    client.close()
    print(f"user_sessions: {NUM_SESSIONS} + 50 дублей")


def seed_event_logs() -> None:
    """Генерация коллекции event_logs."""
    client, db = _get_mongo_db()
    user_ids = _generate_user_ids()

    db.event_logs.drop()

    docs = []
    for i in range(NUM_EVENTS):
        extra = {"error_code": random.randint(400, 503)} if random.random() > 0.7 else None
        docs.append({
            "event_id": f"evt_{i + 1:05d}",
            "timestamp": _rand_date("2024-01-01", "2024-06-30").isoformat() + "Z",
            "event_type": random.choice(EVENT_TYPES),
            "details": {
                "page": random.choice(PAGES),
                "user_id": random.choice(user_ids),
                "extra": extra,
            },
        })

    db.event_logs.insert_many(docs)
    dupes = [{k: v for k, v in d.items() if k != "_id"} for d in docs[:100]]
    db.event_logs.insert_many(dupes)

    client.close()
    print(f"event_logs: {NUM_EVENTS} + 100 дублей")


def seed_support_tickets() -> None:
    """Генерация коллекции support_tickets."""
    client, db = _get_mongo_db()
    user_ids = _generate_user_ids()

    db.support_tickets.drop()

    docs = []
    for i in range(NUM_TICKETS):
        created = _rand_date("2024-01-01", "2024-06-30")
        updated = created + timedelta(minutes=random.randint(10, 10080))
        msgs = []
        msg_time = created
        for m in range(random.randint(1, 5)):
            msg_time = msg_time + timedelta(minutes=random.randint(5, 1440))
            msgs.append({
                "sender": "user" if m % 2 == 0 else "support",
                "message": random.choice(USER_MESSAGES if m % 2 == 0 else SUPPORT_MESSAGES),
                "timestamp": msg_time.isoformat() + "Z",
            })

        docs.append({
            "ticket_id": f"ticket_{i + 1:04d}",
            "user_id": random.choice(user_ids),
            "status": random.choice(STATUSES),
            "issue_type": random.choice(ISSUE_TYPES),
            "messages": msgs,
            "created_at": created.isoformat() + "Z",
            "updated_at": updated.isoformat() + "Z",
        })

    db.support_tickets.insert_many(docs)
    client.close()
    print(f"support_tickets: {NUM_TICKETS}")


def seed_user_recommendations() -> None:
    """Генерация коллекции user_recommendations."""
    client, db = _get_mongo_db()
    user_ids = _generate_user_ids()
    product_ids = _generate_product_ids()

    db.user_recommendations.drop()

    docs = []
    for uid in user_ids:
        prods = random.sample(product_ids, k=random.randint(2, 8))
        docs.append({
            "user_id": uid,
            "recommended_products": prods,
            "last_updated": _rand_date("2024-05-01", "2024-06-30").isoformat() + "Z",
        })

    db.user_recommendations.insert_many(docs)
    client.close()
    print(f"user_recommendations: {len(docs)}")


def seed_moderation_queue() -> None:
    """Генерация коллекции moderation_queue."""
    client, db = _get_mongo_db()
    user_ids = _generate_user_ids()
    product_ids = _generate_product_ids()

    db.moderation_queue.drop()

    docs = []
    for i in range(NUM_REVIEWS):
        review_flags = random.sample(FLAGS, k=random.randint(0, 2))
        docs.append({
            "review_id": f"rev_{i + 1:04d}",
            "user_id": random.choice(user_ids),
            "product_id": random.choice(product_ids),
            "review_text": random.choice(REVIEW_TEXTS),
            "rating": random.randint(1, 5),
            "moderation_status": random.choice(MODERATION_STATUSES),
            "flags": review_flags,
            "submitted_at": _rand_date("2024-01-01", "2024-06-30").isoformat() + "Z",
        })

    db.moderation_queue.insert_many(docs)
    client.close()
    print(f"moderation_queue: {NUM_REVIEWS}")


# ═══════════════════════════════════════════════════════════════
# DAG
# ═══════════════════════════════════════════════════════════════
with DAG(
        dag_id="seed_mongo",
        description="Генерация тестовых данных в MongoDB",
        start_date=datetime(2024, 1, 1),
        schedule=None,  # запуск вручную
        catchup=False,
        tags=["etl", "seed", "mongo"],
) as dag:
    t1 = PythonOperator(task_id="seed_user_sessions", python_callable=seed_user_sessions)
    t2 = PythonOperator(task_id="seed_event_logs", python_callable=seed_event_logs)
    t3 = PythonOperator(task_id="seed_support_tickets", python_callable=seed_support_tickets)
    t4 = PythonOperator(task_id="seed_user_recommendations", python_callable=seed_user_recommendations)
    t5 = PythonOperator(task_id="seed_moderation_queue", python_callable=seed_moderation_queue)

    # Все задачи независимы, выполняются параллельно
    [t1, t2, t3, t4, t5]
