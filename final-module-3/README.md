# Итоговое задание — модуль 3

ETL-процесс: MongoDB → PostgreSQL → аналитические витрины, оркестрация через Airflow.

## Запуск

```bash
sudo chown -R 50000:0 .
docker compose up -d
# Airflow UI: http://localhost:8080/home (admin / admin)
```

## Порядок запуска DAG-ов

1. `seed_mongo` — генерирует данные в 5 коллекциях MongoDB
2. `mongo_to_postgres_replication` — реплицирует в PostgreSQL, трансформирует в clean-слой
3. `build_analytical_marts` — строит 2 витрины

## Коллекции MongoDB (база ecommerce)

- `user_sessions` — 2000 сессий + 50 дублей
- `event_logs` — 5000 событий + 100 дублей
- `support_tickets` — 600 тикетов
- `user_recommendations` — 200 записей
- `moderation_queue` — 800 отзывов

## Слои PostgreSQL

**raw** — данные из MongoDB как есть (5 таблиц)

**clean** — очищенные данные с производными полями:
- `clean_user_sessions` — партиционирована по месяцам, вычислены duration_min, num_pages, num_actions
- `clean_event_logs` — партиционирована по месяцам
- `clean_support_tickets` — вычислены resolution_hours

**mart** — аналитические витрины:
- `mart_user_activity` — активность по user/month (сессии, страницы, устройства, действия)
- `mart_support_efficiency` — тикеты по статусам/типам, время решения

## Трансформации

- Дедупликация: aggregation pipeline в MongoDB + ON CONFLICT в PostgreSQL
- Разворачивание вложенных документов (details в event_logs)
- Фильтрация аномалий (сессии >24ч, тикеты с created_at > updated_at)
- Партиционирование по месяцам

## Проверка

```sql
SELECT 'raw_user_sessions' AS t, COUNT(*) FROM raw_user_sessions
UNION ALL SELECT 'clean_user_sessions', COUNT(*) FROM clean_user_sessions
UNION ALL SELECT 'mart_user_activity', COUNT(*) FROM mart_user_activity
UNION ALL SELECT 'mart_support_efficiency', COUNT(*) FROM mart_support_efficiency;
```
