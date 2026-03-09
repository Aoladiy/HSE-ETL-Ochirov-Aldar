-- init.sql — Инициализация PostgreSQL: таблицы-реплики из MongoDB + аналитические витрины
BEGIN;

SET search_path = public;

-- СЛОЙ RAW: таблицы-реплики данных из MongoDB (staging)
-- 1. UserSessions
DROP TABLE IF EXISTS raw_user_sessions CASCADE;
CREATE TABLE raw_user_sessions
(
    session_id    TEXT PRIMARY KEY,
    user_id       TEXT      NOT NULL,
    start_time    TIMESTAMP NOT NULL,
    end_time      TIMESTAMP NOT NULL,
    pages_visited TEXT[],
    device        TEXT,
    actions       TEXT[],
    loaded_at     TIMESTAMP DEFAULT NOW()
);

-- 2. EventLogs
DROP TABLE IF EXISTS raw_event_logs CASCADE;
CREATE TABLE raw_event_logs
(
    event_id        TEXT PRIMARY KEY,
    event_timestamp TIMESTAMP NOT NULL,
    event_type      TEXT      NOT NULL,
    page            TEXT,
    user_id         TEXT,
    error_code      INT,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

-- 3. SupportTickets
DROP TABLE IF EXISTS raw_support_tickets CASCADE;
CREATE TABLE raw_support_tickets
(
    ticket_id     TEXT PRIMARY KEY,
    user_id       TEXT      NOT NULL,
    status        TEXT      NOT NULL,
    issue_type    TEXT      NOT NULL,
    message_count INT,
    created_at    TIMESTAMP NOT NULL,
    updated_at    TIMESTAMP NOT NULL,
    loaded_at     TIMESTAMP DEFAULT NOW()
);

-- 4. UserRecommendations
DROP TABLE IF EXISTS raw_user_recommendations CASCADE;
CREATE TABLE raw_user_recommendations
(
    user_id              TEXT PRIMARY KEY,
    recommended_products TEXT[],
    num_recommendations  INT,
    last_updated         TIMESTAMP,
    loaded_at            TIMESTAMP DEFAULT NOW()
);

-- 5. ModerationQueue
DROP TABLE IF EXISTS raw_moderation_queue CASCADE;
CREATE TABLE raw_moderation_queue
(
    review_id         TEXT PRIMARY KEY,
    user_id           TEXT NOT NULL,
    product_id        TEXT NOT NULL,
    review_text       TEXT,
    rating            INT CHECK (rating BETWEEN 1 AND 5),
    moderation_status TEXT,
    flags             TEXT[],
    submitted_at      TIMESTAMP,
    loaded_at         TIMESTAMP DEFAULT NOW()
);

-- СЛОЙ CLEAN: очищенные и партиционированные данные
DROP TABLE IF EXISTS clean_user_sessions CASCADE;
CREATE TABLE clean_user_sessions
(
    session_id    TEXT      NOT NULL,
    user_id       TEXT      NOT NULL,
    start_time    TIMESTAMP NOT NULL,
    end_time      TIMESTAMP NOT NULL,
    session_date  DATE      NOT NULL,
    duration_min  NUMERIC(10, 2),
    pages_visited TEXT[],
    num_pages     INT,
    device        TEXT,
    actions       TEXT[],
    num_actions   INT,
    loaded_at     TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (session_id, session_date)
) PARTITION BY RANGE (session_date);

-- Партиции по месяцам
CREATE TABLE clean_user_sessions_2024_01 PARTITION OF clean_user_sessions
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE clean_user_sessions_2024_02 PARTITION OF clean_user_sessions
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE clean_user_sessions_2024_03 PARTITION OF clean_user_sessions
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
CREATE TABLE clean_user_sessions_2024_04 PARTITION OF clean_user_sessions
    FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');
CREATE TABLE clean_user_sessions_2024_05 PARTITION OF clean_user_sessions
    FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');
CREATE TABLE clean_user_sessions_2024_06 PARTITION OF clean_user_sessions
    FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');
CREATE TABLE clean_user_sessions_default PARTITION OF clean_user_sessions
    DEFAULT;

DROP TABLE IF EXISTS clean_event_logs CASCADE;
CREATE TABLE clean_event_logs
(
    event_id        TEXT      NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    event_date      DATE      NOT NULL,
    event_type      TEXT      NOT NULL,
    page            TEXT,
    user_id         TEXT,
    error_code      INT,
    loaded_at       TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (event_id, event_date)
) PARTITION BY RANGE (event_date);

CREATE TABLE clean_event_logs_2024_01 PARTITION OF clean_event_logs
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE clean_event_logs_2024_02 PARTITION OF clean_event_logs
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE clean_event_logs_2024_03 PARTITION OF clean_event_logs
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
CREATE TABLE clean_event_logs_2024_04 PARTITION OF clean_event_logs
    FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');
CREATE TABLE clean_event_logs_2024_05 PARTITION OF clean_event_logs
    FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');
CREATE TABLE clean_event_logs_2024_06 PARTITION OF clean_event_logs
    FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');
CREATE TABLE clean_event_logs_default PARTITION OF clean_event_logs
    DEFAULT;

DROP TABLE IF EXISTS clean_support_tickets CASCADE;
CREATE TABLE clean_support_tickets
(
    ticket_id        TEXT PRIMARY KEY,
    user_id          TEXT      NOT NULL,
    status           TEXT      NOT NULL,
    issue_type       TEXT      NOT NULL,
    message_count    INT,
    created_at       TIMESTAMP NOT NULL,
    updated_at       TIMESTAMP NOT NULL,
    resolution_hours NUMERIC(10, 2),
    loaded_at        TIMESTAMP DEFAULT NOW()
);

-- СЛОЙ MART: аналитические витрины
-- Витрина 1: Активность пользователей
DROP TABLE IF EXISTS mart_user_activity CASCADE;
CREATE TABLE mart_user_activity
(
    user_id               TEXT NOT NULL,
    report_month          DATE NOT NULL,
    total_sessions        INT,
    total_duration_min    NUMERIC(10, 2),
    avg_duration_min      NUMERIC(10, 2),
    total_pages           INT,
    avg_pages_per_session NUMERIC(10, 2),
    total_actions         INT,
    top_device            TEXT,
    top_page              TEXT,
    top_action            TEXT,
    PRIMARY KEY (user_id, report_month)
);

-- Витрина 2: Эффективность поддержки
DROP TABLE IF EXISTS mart_support_efficiency CASCADE;
CREATE TABLE mart_support_efficiency
(
    report_month            DATE NOT NULL,
    issue_type              TEXT NOT NULL,
    total_tickets           INT,
    open_tickets            INT,
    in_progress_tickets     INT,
    resolved_tickets        INT,
    closed_tickets          INT,
    avg_resolution_hours    NUMERIC(10, 2),
    min_resolution_hours    NUMERIC(10, 2),
    max_resolution_hours    NUMERIC(10, 2),
    avg_messages_per_ticket NUMERIC(10, 2),
    PRIMARY KEY (report_month, issue_type)
);

COMMIT;
