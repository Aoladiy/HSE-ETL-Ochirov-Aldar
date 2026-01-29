from __future__ import annotations

import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

BASE_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
OUT_DIR = os.path.join(BASE_DIR, "out")

POSTGRES_CONN_ID = "postgres_default"


def _ensure_dirs() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)


def _get_latest_json_payload() -> dict:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    row = hook.get_first("SELECT payload FROM json_content ORDER BY id DESC LIMIT 1;")
    if not row or row[0] is None:
        raise ValueError("json_content пустая или payload = NULL")

    payload = row[0]
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        return json.loads(payload)
    return json.loads(str(payload))


def _get_latest_xml_payload() -> str:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    row = hook.get_first("SELECT payload::text FROM xml_content ORDER BY id DESC LIMIT 1;")
    if not row or row[0] is None:
        raise ValueError("xml_content пустая или payload = NULL")
    return row[0]


def json_to_linear_from_pg(**_context) -> None:
    payload = _get_latest_json_payload()
    pets = payload.get("pets", [])

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    hook.run("TRUNCATE TABLE pet_favfoods RESTART IDENTITY CASCADE;")
    hook.run("TRUNCATE TABLE pets RESTART IDENTITY CASCADE;")

    for pet in pets:
        name = pet.get("name")
        favs = pet.get("favFoods") or []
        if not isinstance(favs, list):
            favs = [str(favs)]

        row = hook.get_first(
            """
            INSERT INTO pets (name, species, birthyear, photo, favfoods)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING pet_id;
            """,
            parameters=(
                name,
                pet.get("species"),
                pet.get("birthYear"),
                pet.get("photo"),
                ";".join([str(x) for x in favs]) if favs else None,
            ),
        )
        pet_id = row[0]

        for fav in favs:
            hook.run(
                "INSERT INTO pet_favfoods (pet_id, favfood) VALUES (%s, %s);",
                parameters=(pet_id, str(fav)),
            )


def _get_child_text(parent: ET.Element | None, tag: str) -> str | None:
    if parent is None:
        return None
    el = parent.find(tag)
    if el is None or el.text is None:
        return None
    return el.text.strip()


def xml_to_linear_from_pg(**_context) -> None:
    xml_text = _get_latest_xml_payload()
    root = ET.fromstring(xml_text)

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    hook.run("TRUNCATE TABLE nutrition_daily_values RESTART IDENTITY;")
    hook.run("TRUNCATE TABLE nutrition_foods RESTART IDENTITY;")

    daily_values = root.find("daily-values")
    if daily_values is not None:
        for metric in list(daily_values):
            txt = (metric.text or "").strip()
            hook.run(
                """
                INSERT INTO nutrition_daily_values (metric, units, value)
                VALUES (%s, %s, %s);
                """,
                parameters=(
                    metric.tag,
                    metric.attrib.get("units"),
                    float(txt) if txt else None,
                ),
            )

    for food in root.findall("food"):
        serving_el = food.find("serving")
        calories_el = food.find("calories")
        vitamins_el = food.find("vitamins")
        minerals_el = food.find("minerals")

        def t(parent, tag):
            el = parent.find(tag) if parent is not None else None
            return (el.text.strip() if el is not None and el.text else None)

        def num(x):
            return float(x) if x not in (None, "") else None

        hook.run(
            """
            INSERT INTO nutrition_foods (
                name, mfr, serving, serving_units,
                calories_total, calories_fat,
                total_fat, saturated_fat, cholesterol, sodium, carb, fiber, protein,
                vitamin_a, vitamin_c, mineral_ca, mineral_fe
            )
            VALUES (
                %s,%s,%s,%s,
                %s,%s,
                %s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s
            );
            """,
            parameters=(
                t(food, "name"),
                t(food, "mfr"),
                (serving_el.text.strip() if serving_el is not None and serving_el.text else None),
                (serving_el.attrib.get("units") if serving_el is not None else None),

                int(calories_el.attrib.get("total")) if calories_el is not None and calories_el.attrib.get(
                    "total") else None,
                int(calories_el.attrib.get("fat")) if calories_el is not None and calories_el.attrib.get(
                    "fat") else None,

                num(t(food, "total-fat")),
                num(t(food, "saturated-fat")),
                num(t(food, "cholesterol")),
                num(t(food, "sodium")),
                num(t(food, "carb")),
                num(t(food, "fiber")),
                num(t(food, "protein")),

                num(t(vitamins_el, "a")),
                num(t(vitamins_el, "c")),
                num(t(minerals_el, "ca")),
                num(t(minerals_el, "fe")),
            ),
        )


with DAG(
        dag_id="sources_from_postgres_to_linear",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["hw", "postgres", "json", "xml"],
) as dag:
    t_json = PythonOperator(
        task_id="json_to_linear_from_pg",
        python_callable=json_to_linear_from_pg,
    )

    t_xml = PythonOperator(
        task_id="xml_to_linear_from_pg",
        python_callable=xml_to_linear_from_pg,
    )

    [t_json, t_xml]
