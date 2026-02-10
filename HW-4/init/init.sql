BEGIN;

SET search_path = public;

DROP TABLE IF EXISTS public.iot_raw CASCADE;
CREATE TABLE public.iot_raw
(
    id         TEXT,
    room_id    TEXT,
    noted_date TEXT,
    temp       TEXT,
    out_in     TEXT
);

DROP TABLE IF EXISTS public.iot_clean CASCADE;
CREATE TABLE public.iot_clean
(
    id      TEXT,
    room_id TEXT,
    day     DATE,
    temp    NUMERIC
);

DROP TABLE IF EXISTS public.iot_daily CASCADE;
CREATE TABLE public.iot_daily
(
    day      DATE PRIMARY KEY,
    avg_temp NUMERIC
);

DROP TABLE IF EXISTS public.iot_daily_extremes CASCADE;
CREATE TABLE public.iot_daily_extremes
(
    kind     TEXT,
    day      DATE,
    avg_temp NUMERIC,
    rank     INT,
    PRIMARY KEY (kind, rank)
);

COMMIT;
