-- Init TimescaleDB per la dashboard SQL di Q1.
-- Eseguito una sola volta al primo avvio del container (volume vuoto).
-- Il sink JDBC di Flink NON crea tabelle: devono esistere qui.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Tipi allineati al sink JDBC del job Q1:
--   TIMESTAMP(3) -> timestamp (without time zone)
--   BIGINT       -> bigint
--   DOUBLE       -> double precision
-- PRIMARY KEY (window_start, airline): abilita l'upsert lato Flink
-- (INSERT ... ON CONFLICT DO UPDATE) → ri-esecuzioni idempotenti.
CREATE TABLE IF NOT EXISTS q1_results (
    window_start        timestamp        NOT NULL,
    window_end          timestamp,
    airline             text             NOT NULL,
    num_flights         bigint,
    completed           bigint,
    cancelled           bigint,
    diverted            bigint,
    dep_delay_mean      double precision,
    cancellation_rate   double precision,
    late_departure_rate double precision,
    PRIMARY KEY (window_start, airline)
);

-- Hypertable partizionata su window_start (la colonna di partizionamento è
-- presente nella PK, requisito di TimescaleDB per gli indici unique).
SELECT create_hypertable('q1_results', 'window_start', if_not_exists => TRUE);
