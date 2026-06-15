# Итоговое ДЗ — Модуль 4 (ETL-процессы)

**Студент:** Алдар Очиров

## Источники данных

- [Airbnb Open Data](https://www.kaggle.com/datasets/arianazmoudeh/airbnbopendata) — данные об объявлениях Airbnb в Нью-Йорке (~102 000 строк)
- [Daily Temperature of Major Cities](https://www.kaggle.com/datasets/sudalairajkumar/daily-temperature-of-major-cities) — ежедневная температура в городах мира (выборка первых 1 500 000 строк ~60 МБ)

Выборка температурного датасета сделана командой:
```bash
head -n 1500000 city_temperature.csv > city_temperature_gt60mb.csv
```

## Структура репозитория

```
.
├── data/                     # Исходные датасеты (в .gitignore, не в репозитории)
├── task-1/                   # Работа с Yandex DataTransfer
│   ├── create-table.yql      # YQL-скрипт создания таблицы в YDB
│   ├── populate-table/       # Go-программа для загрузки CSV в YDB
│   └── screenshots/
├── task-2/                   # Автоматизация через Airflow + Data Processing
│   ├── data_ingest_dag.py    # DAG файл
│   ├── data-processing.py    # PySpark задание
│   └── screenshots/
├── task-3/                   # Работа с Apache Kafka через PySpark
│   ├── kafka-write.py        # Запись сообщений в топик
│   ├── kafka-read.py         # Чтение и разворачивание JSON
│   └── screenshots/
└── task-4/                   # Дашборд в DataLens
    └── img.png
```

## Задание 1. Работа с Yandex DataTransfer

**Датасет:** Airbnb Open Data

Создана база данных YDB с таблицей `airbnb` (26 колонок, скрипт `task-1/create-table.yql`). Данные загружены из локального CSV файла с помощью Go-программы (`task-1/populate-table/main.go`), которая читает CSV и батчами по 1000 строк вставляет данные через YDB Go SDK.

Настроен трансфер в Yandex DataTransfer типа **Копировать**: источник — YDB, приёмник — Object Storage. Трансфер успешно завершён, результирующий файл размером ~34 МБ сохранён в бакете.

## Задание 2. Автоматизация с Airflow и Yandex Data Processing

**Датасет:** Daily Temperature of Major Cities (выборка ~60 МБ)

Подготовлен DAG (`task-2/data_ingest_dag.py`) в Managed Service for Apache Airflow, который выполняет три этапа:

1. Создание временного кластера Yandex Data Processing
2. Запуск PySpark задания (`task-2/data-processing.py`) — читает CSV из Object Storage, считает среднюю температуру по стране и году, сохраняет результат в parquet
3. Удаление кластера

DAG успешно выполнен, результат сохранён в `s3://default-bucket-1234/temperature-result/`.

## Задание 3. Apache Kafka и PySpark

Настроен кластер Managed Service for Apache Kafka с топиком `dataproc-kafka-topic`.

Созданы два PySpark задания:

- `task-3/kafka-write.py` — генерирует и отправляет ~70 000 JSON сообщений (~28 МБ) с данными о кредитных заявках в топик Kafka
- `task-3/kafka-read.py` — читает сообщения из топика, парсит вложенный JSON и разворачивает в плоский вид (поля: `application_id`, `customer_id`, `region`, `loan_amount`, `term_months`, `score`, `risk_level`, `decision_status`, `submitted_at`, `doc_type`, `doc_status`), сохраняет результат в parquet в Object Storage

## Задание 4. Визуализация в DataLens

Построен дашборд в Yandex DataLens на основе данных Airbnb из YDB с тремя чартами:

- **Количество объявлений по районам** (neighbourhood_group) — столбчатая диаграмма
- **Распределение по типу комнаты** (room_type) — круговая диаграмма
- **Количество объявлений по политике отмены** (cancellation_policy) — столбчатая диаграмма