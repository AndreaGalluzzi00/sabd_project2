# Dashboard real-time Q1 (Opzionale 1)

Visualizzazione in tempo reale delle metriche di Q1 su **Grafana**, con **due backend
alternativi** tenuti volutamente entrambi a fini di confronto nella presentazione.

```
                                  ┌──(topic Kafka q1_results)──► Telegraf ──► InfluxDB ─┐
Flink Q1 ──► vista q1_agg ──┤                                                            ├─► Grafana
            │                     └──(sink JDBC)───────────────────────────► TimescaleDB ┘
            └──(sink CSV, invariato)──► Results/q1 ──► merge_q1.py ──► output certificato
```

Il sink CSV resta **sempre** attivo: le dashboard sono consumatori paralleli. Tutti i
sink leggono la **stessa vista `q1_agg`** nel job Flink → CSV, InfluxDB e TimescaleDB
non possono divergere.

## Perché due stack (per l'orale)

| | InfluxDB | TimescaleDB |
|---|---|---|
| Paradigma | time-series nativo (TSM) | SQL relazionale + estensione time-series |
| Connettore Flink | ❌ assente → **Telegraf** fa da ponte Kafka→Influx | ✅ **JDBC nativo**, sink diretto |
| Componenti runtime | Kafka topic + Telegraf + InfluxDB | solo TimescaleDB |
| Linguaggio Grafana | Flux | SQL (`$__timeFilter`, hypertable) |
| Idempotenza re-run | nuovo punto + `last()` | upsert via PK `(window_start, airline)` |
| Estensione a Q2/Q3 | ranking/lista scomodi | `ORDER BY/LIMIT`, lista in `jsonb` naturali |

Talking point: *time-series-native ma senza connettore Flink* vs *SQL con sink nativo,
meno componenti*. A questa scala (~9.765 righe) le prestazioni sono equivalenti; la
differenza è architetturale.

## Componenti

| Servizio | Profilo | Porta | Note |
|---|---|---|---|
| `influxdb` | `dashboard-influx` | 8086 | org `sabd`, bucket `flights`, retention infinita |
| `telegraf` | `dashboard-influx` | — | consuma `q1_results`, misura `q1`, tag `airline` |
| `dashboard-init` | `dashboard-influx` | — | crea il topic `q1_results` (one-shot) |
| `timescaledb` | `dashboard-timescale` | 5432 | tabella+hypertable create dall'init SQL |
| `grafana` | entrambi | 3000 | provisiona i 2 datasource + le 2 dashboard |

> Credenziali **solo locale/demo** — Grafana `admin/admin` · InfluxDB `admin/admin12345`
> · TimescaleDB `sabd/sabd`.

## Avvio

1. **Abilita i sink** desiderati in `config/base.yml` (uno, l'altro o entrambi):
   ```yaml
   dashboard:
     influx:    { enabled: true }
     timescale: { enabled: true }
   ```

2. **Ricostruisci l'immagine Flink** una sola volta (il `job.py` e i jar JDBC sono
   nell'immagine; il toggle di `base.yml` è montato e non richiede rebuild):
   ```bash
   docker compose build
   ```

3. **Avvia infrastruttura + dashboard** (scegli i profili):
   ```bash
   # solo InfluxDB
   docker compose --profile dashboard-influx up -d
   # solo TimescaleDB
   docker compose --profile dashboard-timescale up -d
   # entrambi, fianco a fianco (consigliato per la demo)
   docker compose --profile dashboard-influx --profile dashboard-timescale up -d
   ```

4. **Esegui la pipeline** come di consueto:
   ```bash
   docker compose run --rm kafka-init        # se manca il topic flights
   docker compose run --rm preprocess        # se manca il parquet
   docker compose run --rm flink-job-q1
   docker compose run --rm producer
   ```

5. **Apri Grafana** → http://localhost:3000 (cartella **SABD**):
   - *SABD - Q1 ... (real-time)* → InfluxDB
   - *SABD - Q1 ... (TimescaleDB)* → TimescaleDB

   L'event-time del replay è **gen–apr 2025**: il time range è già impostato lì.

## Spegnimento

```bash
docker compose --profile dashboard-influx --profile dashboard-timescale down       # ferma
docker compose --profile dashboard-influx --profile dashboard-timescale down -v     # + dati
```

## Tornare alla pipeline "solo CSV"

Rimetti `enabled: false` su entrambi i backend in `config/base.yml` e risottometti il
job: nessun sink dashboard viene creato, la pipeline certificata gira identica. Nessun
rebuild necessario (i flag sono letti a runtime dalla config montata).

## Note tecniche

- **Una sola query Flink, N sink.** Lo `StatementSet` legge la sorgente Kafka una volta
  e fa fan-out della vista `q1_agg` verso CSV (+ Kafka/JDBC se abilitati): un solo job,
  compatibile col workflow `stop --drain` / marker EOS.
- **Timestamp.** InfluxDB usa `window_start` come tempo del punto (via Telegraf);
  TimescaleDB lo riceve come `timestamp` colonna di partizionamento dell'hypertable.
  Entrambi coerenti col CSV perché provengono dalla stessa vista.
- **Pre-esistenza degli oggetti.** Il sink Kafka richiede il topic `q1_results`
  (`dashboard-init`); il sink JDBC richiede la tabella `q1_results` (init SQL): nessuno
  dei due crea l'oggetto a runtime. Per questo i sink sono dietro flag (default off):
  evitano di rompere la pipeline certificata quando lo stack dashboard non è su.
- **Estensione a Q2/Q3.** Stesso pattern, un sink per query. Q2 si rende meglio con un
  *Table panel* (top-10) e la lista `delayed_flights` come stringa/`jsonb`.
