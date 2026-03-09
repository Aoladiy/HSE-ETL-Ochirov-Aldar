"""
Репликация данных из MongoDB в PostgreSQL.
5 коллекций → raw-слой → трансформация → clean-слой.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres_default"
MONGO_URI = "mongodb://etl-mongo:27017"
MONGO_DB = "ecommerce"

default_args = {
    "owner": "student",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _get_mongo_client():
    from pymongo import MongoClient
    return MongoClient(MONGO_URI)


def replicate_user_sessions() -> None:
    """user_sessions: дедупликация через aggregation pipeline → raw_user_sessions."""
    client = _get_mongo_client()
    db = client[MONGO_DB]

    pipeline = [
        {"$group": {"_id": "$session_id", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}}
    ]
    docs = list(db.user_sessions.aggregate(pipeline))
    client.close()

    if not docs:
        print("No user_sessions found")
        return

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE raw_user_sessions;")

    for doc in docs:
        pages = doc.get("pages_visited", [])
        acts = doc.get("actions", [])
        pages_pg = "{" + ",".join(f'"{p}"' for p in pages) + "}" if pages else "{}"
        acts_pg = "{" + ",".join(f'"{a}"' for a in acts) + "}" if acts else "{}"

        cur.execute(
            """INSERT INTO raw_user_sessions
               (session_id, user_id, start_time, end_time, pages_visited, device, actions)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (session_id) DO NOTHING""",
            (doc["session_id"], doc["user_id"], doc["start_time"], doc["end_time"],
             pages_pg, doc.get("device", "unknown"), acts_pg),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Replicated {len(docs)} user_sessions")


def replicate_event_logs() -> None:
    """event_logs: разворачиваем вложенный details, дедупликация → raw_event_logs."""
    client = _get_mongo_client()
    db = client[MONGO_DB]

    pipeline = [
        {"$group": {"_id": "$event_id", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}}
    ]
    docs = list(db.event_logs.aggregate(pipeline))
    client.close()

    if not docs:
        return

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE raw_event_logs;")

    for doc in docs:
        details = doc.get("details", {})
        page = details.get("page") if isinstance(details, dict) else None
        user_id = details.get("user_id") if isinstance(details, dict) else None
        extra = details.get("extra") if isinstance(details, dict) else None
        error_code = extra.get("error_code") if isinstance(extra, dict) else None

        cur.execute(
            """INSERT INTO raw_event_logs
                   (event_id, event_timestamp, event_type, page, user_id, error_code)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (event_id) DO NOTHING""",
            (doc["event_id"], doc["timestamp"], doc["event_type"],
             page, user_id, error_code),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Replicated {len(docs)} event_logs")


def replicate_support_tickets() -> None:
    """support_tickets: считаем message_count из массива messages → raw_support_tickets."""
    client = _get_mongo_client()
    db = client[MONGO_DB]
    docs = list(db.support_tickets.find())
    client.close()

    if not docs:
        return

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE raw_support_tickets;")

    for doc in docs:
        cur.execute(
            """INSERT INTO raw_support_tickets
               (ticket_id, user_id, status, issue_type, message_count, created_at, updated_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (ticket_id) DO NOTHING""",
            (doc["ticket_id"], doc["user_id"], doc["status"], doc["issue_type"],
             len(doc.get("messages", [])), doc["created_at"], doc["updated_at"]),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Replicated {len(docs)} support_tickets")


def replicate_user_recommendations() -> None:
    """user_recommendations → raw_user_recommendations."""
    client = _get_mongo_client()
    db = client[MONGO_DB]
    docs = list(db.user_recommendations.find())
    client.close()

    if not docs:
        return

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE raw_user_recommendations;")

    for doc in docs:
        prods = doc.get("recommended_products", [])
        prods_pg = "{" + ",".join(f'"{p}"' for p in prods) + "}" if prods else "{}"

        cur.execute(
            """INSERT INTO raw_user_recommendations
                   (user_id, recommended_products, num_recommendations, last_updated)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (user_id) DO NOTHING""",
            (doc["user_id"], prods_pg, len(prods), doc.get("last_updated")),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Replicated {len(docs)} user_recommendations")


def replicate_moderation_queue() -> None:
    """moderation_queue → raw_moderation_queue."""
    client = _get_mongo_client()
    db = client[MONGO_DB]
    docs = list(db.moderation_queue.find())
    client.close()

    if not docs:
        return

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE raw_moderation_queue;")

    for doc in docs:
        fl = doc.get("flags", [])
        flags_pg = "{" + ",".join(f'"{f}"' for f in fl) + "}" if fl else "{}"

        cur.execute(
            """INSERT INTO raw_moderation_queue
               (review_id, user_id, product_id, review_text, rating,
                moderation_status, flags, submitted_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (review_id) DO NOTHING""",
            (doc["review_id"], doc["user_id"], doc["product_id"],
             doc.get("review_text"), doc.get("rating"),
             doc.get("moderation_status"), flags_pg, doc.get("submitted_at")),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Replicated {len(docs)} moderation_queue")


def transform_to_clean() -> None:
    """raw → clean: вычисляем производные поля, фильтруем аномалии, партиционируем."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    hook.run("""
             TRUNCATE TABLE clean_user_sessions;
             INSERT INTO clean_user_sessions
             (session_id, user_id, start_time, end_time, session_date,
              duration_min, pages_visited, num_pages, device, actions, num_actions)
             SELECT session_id,
                    user_id,
                    start_time,
                    end_time,
                    start_time::date,
                    ROUND(EXTRACT(EPOCH FROM (end_time - start_time)) / 60.0, 2),
                    pages_visited,
                    COALESCE(array_length(pages_visited, 1), 0),
                    device,
                    actions,
                    COALESCE(array_length(actions, 1), 0)
             FROM raw_user_sessions
             WHERE start_time < end_time
               AND end_time - start_time < INTERVAL '24 hours';
             """)

    hook.run("""
             TRUNCATE TABLE clean_event_logs;
             INSERT INTO clean_event_logs
             (event_id, event_timestamp, event_date, event_type, page, user_id, error_code)
             SELECT event_id,
                    event_timestamp,
                    event_timestamp::date,
                    event_type,
                    page,
                    user_id,
                    error_code
             FROM raw_event_logs
             WHERE event_timestamp IS NOT NULL;
             """)

    hook.run("""
             TRUNCATE TABLE clean_support_tickets;
             INSERT INTO clean_support_tickets
             (ticket_id, user_id, status, issue_type, message_count,
              created_at, updated_at, resolution_hours)
             SELECT ticket_id,
                    user_id,
                    status,
                    issue_type,
                    message_count,
                    created_at,
                    updated_at,
                    ROUND(EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600.0, 2)
             FROM raw_support_tickets
             WHERE created_at <= updated_at;
             """)

    print("transform_to_clean done")


def validate_data() -> None:
    """Проверка что все таблицы не пусты."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    tables = [
        "raw_user_sessions", "raw_event_logs", "raw_support_tickets",
        "raw_user_recommendations", "raw_moderation_queue",
        "clean_user_sessions", "clean_event_logs", "clean_support_tickets",
    ]
    for table in tables:
        count = hook.get_records(f"SELECT COUNT(*) FROM {table};")[0][0]
        print(f"  {table}: {count} rows")
        if count == 0:
            raise ValueError(f"{table} is empty!")
    print("Validation passed")


with DAG(
        dag_id="mongo_to_postgres_replication",
        description="Репликация MongoDB → PostgreSQL + трансформация",
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False,
        default_args=default_args,
        tags=["etl", "replication"],
) as dag:
    t_sessions = PythonOperator(task_id="replicate_user_sessions", python_callable=replicate_user_sessions)
    t_events = PythonOperator(task_id="replicate_event_logs", python_callable=replicate_event_logs)
    t_tickets = PythonOperator(task_id="replicate_support_tickets", python_callable=replicate_support_tickets)
    t_recs = PythonOperator(task_id="replicate_user_recommendations", python_callable=replicate_user_recommendations)
    t_moderation = PythonOperator(task_id="replicate_moderation_queue", python_callable=replicate_moderation_queue)
    t_transform = PythonOperator(task_id="transform_to_clean", python_callable=transform_to_clean)
    t_validate = PythonOperator(task_id="validate_data", python_callable=validate_data)

    [t_sessions, t_events, t_tickets, t_recs, t_moderation] >> t_transform >> t_validate
