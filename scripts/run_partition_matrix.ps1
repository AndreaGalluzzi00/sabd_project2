<#
run_partition_matrix.ps1

Sweep parametrizzabile parallelismo Flink x partizioni Kafka, per UNA query,
in regime COMPUTE-BOUND (backlog pre-caricato).

Per ogni combo (p, n):
  1. ricrea il topic 'flights' con n partizioni (delete + create, RF configurabile);
  2. pre-carica il topic con i 2.2M eventi (il producer riempie perche' e' vuoto);
  3. esegue la query a parallelismo p leggendo il backlog (-NoResetTopic: il
     producer salta perche' il topic e' pieno);
  4. report_perf aggiunge una riga (experiment = pP_nN) a Results/perf.csv.

Richiede i config config/experiments/pP_nN.yml (estendono perf_cbP e rinominano).
Le partizioni NON sono nel config: le imposta questo script alla creazione del topic.

USO (dalla root del progetto):
  powershell -ExecutionPolicy Bypass -File .\scripts\run_partition_matrix.ps1 -Query q3
  powershell -ExecutionPolicy Bypass -File .\scripts\run_partition_matrix.ps1 -Query q2 -Combos "1:1","2:2","4:4"
  powershell -ExecutionPolicy Bypass -File .\scripts\run_partition_matrix.ps1 -Query q1 -ReplicationFactor 2

VALIDITA': una riga e' valida se total_records ~= 2.229.45x (x = n partizioni,
per i marker EOS). Valori piccoli (1, 50000, 157091...) = run fallita (OOM o
troncamento) -> per q2/q3 servono i fix (direct memory del TM + cap 180s).
#>
param(
    [ValidateSet("q1", "q2", "q3")]
    [string]$Query = "q1",

    # Combo come "parallelismo:partizioni".
    [string[]]$Combos = @("1:1", "2:1", "2:2", "4:1", "4:2", "4:4"),

    [int]$ReplicationFactor = 1
)

$ErrorActionPreference = "Stop"

# Vai alla root del progetto (questo script sta in scripts/).
$ProjectRoot = Split-Path $PSScriptRoot -Parent
Set-Location $ProjectRoot

if (-not (Test-Path ".\docker-compose.yml")) {
    throw "docker-compose.yml non trovato in $ProjectRoot"
}


function Reset-FlightsTopic {
    param([int]$Partitions)

    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 `
        --delete --topic flights --if-exists
    Start-Sleep -Seconds 3
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 `
        --create --topic flights --partitions $Partitions --replication-factor $ReplicationFactor
}


foreach ($combo in $Combos) {
    $parts = $combo.Split(":")
    if ($parts.Count -ne 2) {
        Write-Warning "Combo '$combo' non valida (usa 'p:n'). Saltata."
        continue
    }
    $p = [int]$parts[0]
    $n = [int]$parts[1]
    $exp = "p${p}_n${n}"
    $cfgHost = ".\config\experiments\$exp.yml"

    if (-not (Test-Path $cfgHost)) {
        Write-Warning "Config mancante: $cfgHost -> combo $combo saltata (creala che estende perf_cb$p)."
        continue
    }

    Write-Host ""
    Write-Host "########## $exp  (parallelismo=$p, partizioni=$n, RF=$ReplicationFactor) - $Query ##########"

    # 1) topic con n partizioni
    Reset-FlightsTopic -Partitions $n

    # 2) pre-carica il topic
    docker compose run --rm -e CONFIG_PATH=/config/experiments/$exp.yml producer

    # 3) esegui la query a parallelismo p sul backlog
    .\scripts\run_experiment.ps1 -e $exp -Query $Query -NoResetTopic -NoPreprocess -NoMerge
}

Write-Host ""
Write-Host "########## MATRICE $Query COMPLETATA ##########"
Write-Host "Risultati: Results\perf.csv (righe pP_nN, colonna query=$Query)."
