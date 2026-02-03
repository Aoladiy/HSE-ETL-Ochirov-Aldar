from __future__ import annotations

from datetime import datetime
from io import StringIO
from urllib.request import urlopen

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres_default"

CSV_URL = "https://raw.githubusercontent.com/sushantdhumak/Temperature-Forecasting-using-IoT-Data/master/IOT-temp.csv"


def load_csv_via_http_to_raw() -> None:
    with urlopen(CSV_URL) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Failed to download CSV. HTTP status: {resp.status}")
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


def transform_and_compute() -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    hook.run(
        """
        TRUNCATE TABLE public.iot_clean;
        TRUNCATE TABLE public.iot_daily;
        TRUNCATE TABLE public.iot_daily_extremes;

        WITH base AS (SELECT id,
                             room_id,
                             -- date format
                             COALESCE(
                                     to_timestamp(noted_date, 'DD-MM-YYYY HH24:MI:SS'),
                                     to_timestamp(noted_date, 'DD-MM-YYYY HH24:MI')
                             )::date                                      AS day,
                             NULLIF(replace(temp, ',', '.'), '')::numeric AS temp_num
                      FROM public.iot_raw
                      -- filtering
                      WHERE lower(trim(out_in)) = 'in'
                        AND noted_date IS NOT NULL
                        AND noted_date <> ''
                        AND temp IS NOT NULL
                        AND temp <> ''),
             -- percentiles
             p AS (SELECT percentile_cont(0.05) WITHIN GROUP (ORDER BY temp_num) AS p5,
                          percentile_cont(0.95) WITHIN GROUP (ORDER BY temp_num) AS p95
                   FROM base)
        INSERT
        INTO public.iot_clean (id, room_id, day, temp)
        SELECT b.id, b.room_id, b.day, b.temp_num
        FROM base b
                 CROSS JOIN p
        WHERE b.temp_num BETWEEN p.p5 AND p.p95;

        -- avg by day
        INSERT INTO public.iot_daily (day, avg_temp)
        SELECT day, AVG(temp) AS avg_temp
        FROM public.iot_clean
        GROUP BY day;

        -- top 5 hot
        INSERT INTO public.iot_daily_extremes (kind, day, avg_temp, rank)
        SELECT 'hot',
               day,
               avg_temp,
               ROW_NUMBER() OVER (ORDER BY avg_temp DESC, day ASC)
        FROM public.iot_daily
        ORDER BY avg_temp DESC, day ASC
        LIMIT 5;

        -- top 5 cold
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
        dag_id="iot_temperature_http_etl",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["hw", "iot", "postgres", "http"],
) as dag:
    t_load = PythonOperator(
        task_id="load_csv_via_http_to_raw",
        python_callable=load_csv_via_http_to_raw,
    )

    t_transform = PythonOperator(
        task_id="transform_and_compute",
        python_callable=transform_and_compute,
    )

    t_load >> t_transform
