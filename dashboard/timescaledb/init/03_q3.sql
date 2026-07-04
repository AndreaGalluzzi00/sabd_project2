-- Init TimescaleDB per la dashboard SQL di Q3 (distribuzione dei ritardi
-- per compagnia e fascia oraria, percentili DDSketch).
-- Eseguito solo al primo avvio del container (volume vuoto): con un volume
-- già inizializzato serve 'docker compose --profile dashboard-timescale down -v'
-- oppure l'esecuzione manuale via psql.
-- Il sink JDBC di Flink NON crea tabelle: devono esistere qui.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Tipi allineati al sink JDBC del job Q3 (q3/window_ops.py, JDBC_SINK_DDL):
--   TIMESTAMP(3) -> timestamp, INT -> int, BIGINT -> bigint, DOUBLE -> double precision
-- PRIMARY KEY (ts, airline, hour): upsert lato Flink → ri-esecuzioni idempotenti.
-- delay_min/delay_max al posto di min/max (parole riservate nel DDL Flink).

CREATE TABLE IF NOT EXISTS q3_results_1d (
    ts          timestamp        NOT NULL,
    airline     text             NOT NULL,
    hour        int              NOT NULL,
    num_flights bigint,
    delay_min   double precision,
    p25         double precision,
    p50         double precision,
    p75         double precision,
    p90         double precision,
    delay_max   double precision,
    PRIMARY KEY (ts, airline, hour)
);

CREATE TABLE IF NOT EXISTS q3_results_7d (
    ts          timestamp        NOT NULL,
    airline     text             NOT NULL,
    hour        int              NOT NULL,
    num_flights bigint,
    delay_min   double precision,
    p25         double precision,
    p50         double precision,
    p75         double precision,
    p90         double precision,
    delay_max   double precision,
    PRIMARY KEY (ts, airline, hour)
);

CREATE TABLE IF NOT EXISTS q3_results_global (
    ts          timestamp        NOT NULL,
    airline     text             NOT NULL,
    hour        int              NOT NULL,
    num_flights bigint,
    delay_min   double precision,
    p25         double precision,
    p50         double precision,
    p75         double precision,
    p90         double precision,
    delay_max   double precision,
    PRIMARY KEY (ts, airline, hour)
);

SELECT create_hypertable('q3_results_1d',     'ts', if_not_exists => TRUE);
SELECT create_hypertable('q3_results_7d',     'ts', if_not_exists => TRUE);
SELECT create_hypertable('q3_results_global', 'ts', if_not_exists => TRUE);
