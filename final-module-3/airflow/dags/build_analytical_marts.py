"""
Построение аналитических витрин из clean-слоя.
Витрина 1: активность пользователей.
Витрина 2: эффективность поддержки.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres_default"

default_args = {
    "owner": "student",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def build_mart_user_activity() -> None:
    """Витрина активности: сессии, страницы, действия, устройства по user/month."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    hook.run("""
             TRUNCATE TABLE mart_user_activity;

             WITH session_stats AS (SELECT user_id,
                                           DATE_TRUNC('month', session_date)::date AS report_month,
                                           COUNT(*)                                AS total_sessions,
                                           SUM(duration_min)                       AS total_duration_min,
                                           ROUND(AVG(duration_min), 2)             AS avg_duration_min,
                                           SUM(num_pages)                          AS total_pages,
                                           ROUND(AVG(num_pages::numeric), 2)       AS avg_pages_per_session,
                                           SUM(num_actions)                        AS total_actions
                                    FROM clean_user_sessions
                                    GROUP BY user_id, DATE_TRUNC('month', session_date)),
                  device_ranked AS (SELECT user_id,
                                           DATE_TRUNC('month', session_date)::date AS report_month,
                                           device,
                                           ROW_NUMBER() OVER (
                                               PARTITION BY user_id, DATE_TRUNC('month', session_date)
                                               ORDER BY COUNT(*) DESC
                                               )                                   AS rn
                                    FROM clean_user_sessions
                                    GROUP BY user_id, DATE_TRUNC('month', session_date), device),
                  page_ranked AS (SELECT user_id,
                                         DATE_TRUNC('month', session_date)::date AS report_month,
                                         page_val,
                                         ROW_NUMBER() OVER (
                                             PARTITION BY user_id, DATE_TRUNC('month', session_date)
                                             ORDER BY COUNT(*) DESC
                                             )                                   AS rn
                                  FROM clean_user_sessions,
                                       LATERAL UNNEST(pages_visited) AS page_val
                                  GROUP BY user_id, DATE_TRUNC('month', session_date), page_val),
                  action_ranked AS (SELECT user_id,
                                           DATE_TRUNC('month', session_date)::date AS report_month,
                                           action_val,
                                           ROW_NUMBER() OVER (
                                               PARTITION BY user_id, DATE_TRUNC('month', session_date)
                                               ORDER BY COUNT(*) DESC
                                               )                                   AS rn
                                    FROM clean_user_sessions,
                                         LATERAL UNNEST(actions) AS action_val
                                    GROUP BY user_id, DATE_TRUNC('month', session_date), action_val)
             INSERT
             INTO mart_user_activity
             (user_id, report_month, total_sessions, total_duration_min,
              avg_duration_min, total_pages, avg_pages_per_session,
              total_actions, top_device, top_page, top_action)
             SELECT s.user_id,
                    s.report_month,
                    s.total_sessions,
                    s.total_duration_min,
                    s.avg_duration_min,
                    s.total_pages,
                    s.avg_pages_per_session,
                    s.total_actions,
                    d.device,
                    p.page_val,
                    a.action_val
             FROM session_stats s
                      LEFT JOIN device_ranked d
                                ON s.user_id = d.user_id AND s.report_month = d.report_month AND d.rn = 1
                      LEFT JOIN page_ranked p ON s.user_id = p.user_id AND s.report_month = p.report_month AND p.rn = 1
                      LEFT JOIN action_ranked a
                                ON s.user_id = a.user_id AND s.report_month = a.report_month AND a.rn = 1;
             """)

    count = hook.get_records("SELECT COUNT(*) FROM mart_user_activity;")[0][0]
    print(f"mart_user_activity: {count} rows")


def build_mart_support_efficiency() -> None:
    """Витрина поддержки: тикеты по статусам/типам, время решения."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    hook.run("""
             TRUNCATE TABLE mart_support_efficiency;

             INSERT INTO mart_support_efficiency
             (report_month, issue_type, total_tickets,
              open_tickets, in_progress_tickets, resolved_tickets, closed_tickets,
              avg_resolution_hours, min_resolution_hours, max_resolution_hours,
              avg_messages_per_ticket)
             SELECT DATE_TRUNC('month', created_at)::date,
                    issue_type,
                    COUNT(*),
                    COUNT(*) FILTER (WHERE status = 'open'),
                    COUNT(*) FILTER (WHERE status = 'in_progress'),
                    COUNT(*) FILTER (WHERE status = 'resolved'),
                    COUNT(*) FILTER (WHERE status = 'closed'),
                    ROUND(AVG(resolution_hours), 2),
                    ROUND(MIN(resolution_hours), 2),
                    ROUND(MAX(resolution_hours), 2),
                    ROUND(AVG(message_count::numeric), 2)
             FROM clean_support_tickets
             GROUP BY DATE_TRUNC('month', created_at), issue_type
             ORDER BY 1, 2;
             """)

    count = hook.get_records("SELECT COUNT(*) FROM mart_support_efficiency;")[0][0]
    print(f"mart_support_efficiency: {count} rows")


def validate_marts() -> None:
    """Проверка что витрины не пусты."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    for table in ["mart_user_activity", "mart_support_efficiency"]:
        count = hook.get_records(f"SELECT COUNT(*) FROM {table};")[0][0]
        print(f"  {table}: {count} rows")
        if count == 0:
            raise ValueError(f"{table} is empty!")
    print("Marts validated")


with DAG(
        dag_id="build_analytical_marts",
        description="Аналитические витрины из clean-слоя",
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False,
        default_args=default_args,
        tags=["etl", "marts"],
) as dag:
    t_activity = PythonOperator(task_id="build_mart_user_activity", python_callable=build_mart_user_activity)
    t_support = PythonOperator(task_id="build_mart_support_efficiency", python_callable=build_mart_support_efficiency)
    t_validate = PythonOperator(task_id="validate_marts", python_callable=validate_marts)

    [t_activity, t_support] >> t_validate
