-- Init TimescaleDB per la dashboard SQL di Q2 (top-10 aeroporti in ritardo).
-- Eseguito solo al primo avvio del container (volume vuoto): con un volume
-- già inizializzato serve 'docker compose --profile dashboard-timescale down -v'
-- oppure l'esecuzione manuale via psql.
-- Il sink JDBC di Flink NON crea tabelle: devono esistere qui.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Tipi allineati al sink JDBC del job Q2 (q2/window_ops.py, JDBC_SINK_DDL).
-- PRIMARY KEY (ts, airport_rank): upsert lato Flink → ri-esecuzioni idempotenti.

CREATE TABLE IF NOT EXISTS q2_results_1h (
    ts                timestamp        NOT NULL,
    airport_rank      bigint           NOT NULL,
    origin_airport_id int,
    num_flights       bigint,
    severe_delays     bigint,
    dep_delay_mean    double precision,
    dep_delay_max     double precision,
    delayed_flights   text,
    PRIMARY KEY (ts, airport_rank)
);

CREATE TABLE IF NOT EXISTS q2_results_6h (
    ts                timestamp        NOT NULL,
    airport_rank      bigint           NOT NULL,
    origin_airport_id int,
    num_flights       bigint,
    severe_delays     bigint,
    dep_delay_mean    double precision,
    dep_delay_max     double precision,
    delayed_flights   text,
    PRIMARY KEY (ts, airport_rank)
);

CREATE TABLE IF NOT EXISTS q2_results_global (
    ts                timestamp        NOT NULL,
    airport_rank      bigint           NOT NULL,
    origin_airport_id int,
    num_flights       bigint,
    severe_delays     bigint,
    dep_delay_mean    double precision,
    dep_delay_max     double precision,
    delayed_flights   text,
    PRIMARY KEY (ts, airport_rank)
);

CREATE TABLE IF NOT EXISTS q2_results_cumulative (
    ts                timestamp        NOT NULL,
    airport_rank      bigint           NOT NULL,
    origin_airport_id int,
    num_flights       bigint,
    severe_delays     bigint,
    dep_delay_mean    double precision,
    dep_delay_max     double precision,
    delayed_flights   text,
    PRIMARY KEY (ts, airport_rank)
);

SELECT create_hypertable('q2_results_1h',     'ts', if_not_exists => TRUE);
SELECT create_hypertable('q2_results_6h',     'ts', if_not_exists => TRUE);
SELECT create_hypertable('q2_results_global', 'ts', if_not_exists => TRUE);
SELECT create_hypertable('q2_results_cumulative', 'ts', if_not_exists => TRUE);
