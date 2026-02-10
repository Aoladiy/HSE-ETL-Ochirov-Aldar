from __future__ import annotations

from datetime import date, datetime, timedelta
from io import StringIO
from urllib.request import urlopen

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres_default"

CSV_URL = "https://raw.githubusercontent.com/sushantdhumak/Temperature-Forecasting-using-IoT-Data/master/IOT-temp.csv"

DAYS_BACK = 7


def load_csv_via_http_to_raw() -> None:
    with urlopen(CSV_URL) as resp:
        if getattr(resp, "status", 200) != 200:
            raise RuntimeError(f"Failed to download CSV. HTTP status: {getattr(resp, 'status', 'unknown')}")
        content = resp.read().decode("utf-8")

    csv_buf = StringIO(content)

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE public.iot_raw;")
        cur.copy_expert(
            """
            COPY public.iot_raw (id, room_id, noted_date, temp, out_in)
            FROM STDIN WITH (FORMAT csv, HEADER true);
            """,
            csv_buf,
        )
    conn.commit()


# полностью
def transform_and_compute_full() -> None:
    """
    Полная прогрузка истории (full refresh):
    - чистим iot_clean, iot_daily, iot_daily_extremes
    - строим clean из raw (In + парсинг даты + процентили)
    - daily (avg per day)
    - extremes (top-5 hot/cold)
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    hook.run(
        """
        TRUNCATE TABLE public.iot_clean;
        TRUNCATE TABLE public.iot_daily;
        TRUNCATE TABLE public.iot_daily_extremes;

        WITH base AS (SELECT id,
                             room_id,
                             COALESCE(
                                     to_timestamp(noted_date, 'DD-MM-YYYY HH24:MI:SS'),
                                     to_timestamp(noted_date, 'DD-MM-YYYY HH24:MI')
                             )::date                                      AS day,
                             NULLIF(replace(temp, ',', '.'), '')::numeric AS temp_num
                      FROM public.iot_raw
                      WHERE lower(trim(out_in)) = 'in'
                        AND noted_date IS NOT NULL
                        AND noted_date <> ''
                        AND temp IS NOT NULL
                        AND temp <> ''),
             p AS (SELECT percentile_cont(0.05) WITHIN GROUP (ORDER BY temp_num) AS p5,
                          percentile_cont(0.95) WITHIN GROUP (ORDER BY temp_num) AS p95
                   FROM base)
        INSERT
        INTO public.iot_clean (id, room_id, day, temp)
        SELECT b.id, b.room_id, b.day, b.temp_num
        FROM base b
                 CROSS JOIN p
        WHERE b.temp_num BETWEEN p.p5 AND p.p95;

        INSERT INTO public.iot_daily (day, avg_temp)
        SELECT day, AVG(temp) AS avg_temp
        FROM public.iot_clean
        GROUP BY day;

        INSERT INTO public.iot_daily_extremes (kind, day, avg_temp, rank)
        SELECT 'hot',
               day,
               avg_temp,
               ROW_NUMBER() OVER (ORDER BY avg_temp DESC, day ASC)
        FROM public.iot_daily
        ORDER BY avg_temp DESC, day ASC
        LIMIT 5;

        INSERT INTO public.iot_daily_extremes (kind, day, avg_temp, rank)
        SELECT 'cold',
               day,
               avg_temp,
               ROW_NUMBER() OVER (ORDER BY avg_temp ASC, day ASC)
        FROM public.iot_daily
        ORDER BY avg_temp ASC, day ASC
        LIMIT 5;
        """
    )


# последние несколько дней
def transform_and_compute_incremental(days_back: int = DAYS_BACK) -> None:
    """
    Инкрементальная прогрузка: пересобираем только последние N дней.
    Чтобы процентильная чистка совпадала с full-load логикой, p5/p95 считаем по ВСЕМ данным (In),
    а пересборку делаем только для day >= cutoff.
    """
    cutoff = date.today() - timedelta(days=days_back)
    cutoff_sql = f"DATE '{cutoff.isoformat()}'"

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    sql = f"""
    -- пороги по всему массиву (In)
    WITH base_all AS (
        SELECT
            COALESCE(
                to_timestamp(noted_date, 'DD-MM-YYYY HH24:MI:SS'),
                to_timestamp(noted_date, 'DD-MM-YYYY HH24:MI')
            )::date AS day,
            NULLIF(replace(temp, ',', '.'), '')::numeric AS temp_num
        FROM public.iot_raw
        WHERE lower(trim(out_in)) = 'in'
          AND noted_date IS NOT NULL AND noted_date <> ''
          AND temp IS NOT NULL AND temp <> ''
    ),
    p AS (
        SELECT
            percentile_cont(0.05) WITHIN GROUP (ORDER BY temp_num) AS p5,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY temp_num) AS p95
        FROM base_all
    ),
    base_recent AS (
        SELECT
            id,
            room_id,
            COALESCE(
                to_timestamp(noted_date, 'DD-MM-YYYY HH24:MI:SS'),
                to_timestamp(noted_date, 'DD-MM-YYYY HH24:MI')
            )::date AS day,
            NULLIF(replace(temp, ',', '.'), '')::numeric AS temp_num
        FROM public.iot_raw
        WHERE lower(trim(out_in)) = 'in'
          AND noted_date IS NOT NULL AND noted_date <> ''
          AND temp IS NOT NULL AND temp <> ''
          AND COALESCE(
                to_timestamp(noted_date, 'DD-MM-YYYY HH24:MI:SS'),
                to_timestamp(noted_date, 'DD-MM-YYYY HH24:MI')
              )::date >= {cutoff_sql}
    )
    SELECT 1;
    """

    hook.run(f"DELETE FROM public.iot_clean WHERE day >= {cutoff_sql};")
    hook.run(f"DELETE FROM public.iot_daily WHERE day >= {cutoff_sql};")

    # clean только за последние N дней, используя p5/p95 по всему датасету
    hook.run(
        f"""
        WITH base_all AS (
            SELECT
                NULLIF(replace(temp, ',', '.'), '')::numeric AS temp_num
            FROM public.iot_raw
            WHERE lower(trim(out_in)) = 'in'
              AND noted_date IS NOT NULL AND noted_date <> ''
              AND temp IS NOT NULL AND temp <> ''
        ),
        p AS (
            SELECT
                percentile_cont(0.05) WITHIN GROUP (ORDER BY temp_num) AS p5,
                percentile_cont(0.95) WITHIN GROUP (ORDER BY temp_num) AS p95
            FROM base_all
        ),
        base_recent AS (
            SELECT
                id,
                room_id,
                COALESCE(
                    to_timestamp(noted_date, 'DD-MM-YYYY HH24:MI:SS'),
                    to_timestamp(noted_date, 'DD-MM-YYYY HH24:MI')
                )::date AS day,
                NULLIF(replace(temp, ',', '.'), '')::numeric AS temp_num
            FROM public.iot_raw
            WHERE lower(trim(out_in)) = 'in'
              AND noted_date IS NOT NULL AND noted_date <> ''
              AND temp IS NOT NULL AND temp <> ''
              AND COALESCE(
                    to_timestamp(noted_date, 'DD-MM-YYYY HH24:MI:SS'),
                    to_timestamp(noted_date, 'DD-MM-YYYY HH24:MI')
                  )::date >= {cutoff_sql}
        )
        INSERT INTO public.iot_clean (id, room_id, day, temp)
        SELECT b.id, b.room_id, b.day, b.temp_num
        FROM base_recent b
        CROSS JOIN p
        WHERE b.temp_num BETWEEN p.p5 AND p.p95;
        """
    )

    # daily только для диапазона
    hook.run(
        f"""
        INSERT INTO public.iot_daily (day, avg_temp)
        SELECT day, AVG(temp) AS avg_temp
        FROM public.iot_clean
        WHERE day >= {cutoff_sql}
        GROUP BY day
        ON CONFLICT (day) DO UPDATE SET avg_temp = EXCLUDED.avg_temp;
        """
    )

    # Extremes пересчитываем полностью
    hook.run("TRUNCATE TABLE public.iot_daily_extremes;")

    hook.run(
        """
        INSERT INTO public.iot_daily_extremes (kind, day, avg_temp, rank)
        SELECT 'hot',
               day,
               avg_temp,
               ROW_NUMBER() OVER (ORDER BY avg_temp DESC, day ASC)
        FROM public.iot_daily
        ORDER BY avg_temp DESC, day ASC
        LIMIT 5;
        """
    )

    hook.run(
        """
        INSERT INTO public.iot_daily_extremes (kind, day, avg_temp, rank)
        SELECT 'cold',
               day,
               avg_temp,
               ROW_NUMBER() OVER (ORDER BY avg_temp ASC, day ASC)
        FROM public.iot_daily
        ORDER BY avg_temp ASC, day ASC
        LIMIT 5;
        """
    )


with DAG(
        dag_id="iot_temperature_full_load",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["hw", "iot", "full"],
) as dag_full:
    t_load_full = PythonOperator(
        task_id="load_csv_via_http_to_raw",
        python_callable=load_csv_via_http_to_raw,
    )

    t_transform_full = PythonOperator(
        task_id="transform_and_compute_full",
        python_callable=transform_and_compute_full,
    )

    t_load_full >> t_transform_full

with DAG(
        dag_id="iot_temperature_incremental_last_days",
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["hw", "iot", "incremental"],
) as dag_incr:
    t_load_incr = PythonOperator(
        task_id="load_csv_via_http_to_raw",
        python_callable=load_csv_via_http_to_raw,
    )

    t_transform_incr = PythonOperator(
        task_id="transform_and_compute_incremental_last_days",
        python_callable=transform_and_compute_incremental,
        op_kwargs={"days_back": DAYS_BACK},
    )

    t_load_incr >> t_transform_incr
