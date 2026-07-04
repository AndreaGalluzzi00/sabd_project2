# Dashboard real-time Q1 e Q3 (Opzionale 1)

Visualizzazione in tempo reale delle metriche di Q1 e Q3 su **Grafana**, con **due
backend alternativi** tenuti volutamente entrambi a fini di confronto nella presentazione.

```
                                  ┌──(topic Kafka q*_results*)─► Telegraf ──► InfluxDB ─┐
Flink Q1/Q3 ──► stessa query ─┤                                                          ├─► Grafana
            │                     └──(sink JDBC)───────────────────────────► TimescaleDB ┘
            └──(sink CSV, invariato)──► Results/q* ──► merge_q*.py ──► output certificato
```

Il sink CSV resta **sempre** attivo: le dashboard sono consumatori paralleli. Per Q1
tutti i sink leggono la **stessa vista `q1_agg`**; per Q3 leggono lo **stesso
DataStream di risultati** (sketch DDSketch) → CSV, InfluxDB e TimescaleDB non possono
divergere.

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
| `telegraf` | `dashboard-influx` | — | consuma `q1_results` (misura `q1`, tag `airline`) e `q3_results_{1d,7d,global}` (misure `q3_*`, tag `airline`+`hour`) |
| `dashboard-init` | `dashboard-influx` | — | crea i topic dei risultati Q1/Q2/Q3 (one-shot) |
| `timescaledb` | `dashboard-timescale` | 5432 | tabelle+hypertable create dagli init SQL (`01_q1`, `02_q2`, `03_q3`) |
| `grafana` | entrambi | 3000 | provisiona i 2 datasource + le 4 dashboard (Q1 e Q3, per ciascun backend) |

> **Nota (init TimescaleDB):** gli script in `timescaledb/init/` girano solo al primo
> avvio del container (volume vuoto). Se il volume `timescaledb_data` esiste già da un
> run precedente all'aggiunta di Q2/Q3, ricrearlo con
> `docker compose --profile dashboard-timescale down -v` oppure eseguire a mano gli
> init: `docker exec -i sabd2-timescaledb psql -U sabd -d sabd < dashboard/timescaledb/init/03_q3.sql`.

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
   docker compose run --rm flink-job-q3
   docker compose run --rm producer
   ```

5. **Apri Grafana** → http://localhost:3000 (cartella **SABD**):
   - *SABD - Q1 ... (real-time)* / *SABD - Q1 ... (TimescaleDB)*
   - *SABD - Q3 ... (real-time)* / *SABD - Q3 ... (TimescaleDB)* — variabili
     in alto per scegliere compagnia e fascia oraria; il pannello "dall'inizio
     del dataset" si popola alla chiusura della finestra globale (marker EOS).

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
- **Pre-esistenza degli oggetti.** I sink Kafka richiedono i topic dei risultati
  (`dashboard-init`); i sink JDBC richiedono le tabelle (init SQL): nessuno dei due
  crea l'oggetto a runtime. Per questo i sink sono dietro flag (default off):
  evitano di rompere la pipeline certificata quando lo stack dashboard non è su.
- **Q3 su InfluxDB.** `hour` è emesso come **stringa** dal sink Kafka di Q3 perché il
  parser JSON di Telegraf accetta solo stringhe come tag; su TimescaleDB resta `int`.
  Una misurazione per finestra (`q3_1d`, `q3_7d`, `q3_global`), punto identificato da
  (tag `airline`+`hour`, time = inizio finestra) → re-run idempotenti su entrambi i backend.
- **Estensione a Q2.** Stesso pattern, un sink per query (topic e tabelle sono già
  provisionati). Q2 si rende meglio con un *Table panel* (top-10) e la lista
  `delayed_flights` come stringa/`jsonb`.
