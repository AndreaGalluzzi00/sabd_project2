<#
run_flink_scalability_suite.ps1

Esegue la matrice di scalabilita' Flink mantenendo Kafka fisso a 4 partizioni.

Scenari predefiniti:
  k4_p1_tm1  -> parallelism 1, 1 TaskManager, 1 slot/TM
  k4_p2_tm1  -> parallelism 2, 1 TaskManager, 2 slot/TM
  k4_p4_tm1  -> parallelism 4, 1 TaskManager, 4 slot/TM
  k4_p2_tm2  -> parallelism 2, 2 TaskManager, 1 slot/TM
  k4_p4_tm2  -> parallelism 4, 2 TaskManager, 2 slot/TM

Con i default esegue 5 scenari x q1/q2/q3 = 15 job. Per ogni scenario il
topic viene ricreato e precaricato una sola volta; le tre query lo rileggono
con consumer group runtime distinti. Il producer non viene modificato.

Dopo il drain verificato della source, run_experiment applica soltanto un'attesa
prudenziale configurabile prima di merge e cancellazione.

USO:
  powershell -ExecutionPolicy Bypass -File .\scripts\run_flink_scalability_suite.ps1
  .\scripts\run_flink_scalability_suite.ps1 -Queries q3 -NoPreprocess
  .\scripts\run_flink_scalability_suite.ps1 `
      -Scenarios @("k4_p2_tm1", "k4_p2_tm2") `
      -Queries @("q1", "q2")
#>
param(
    [ValidateSet("q1", "q2", "q3")]
    [string[]]$Queries = @("q1", "q2", "q3"),

    [ValidateSet(
        "k4_p1_tm1",
        "k4_p2_tm1",
        "k4_p4_tm1",
        "k4_p2_tm2",
        "k4_p4_tm2"
    )]
    [string[]]$Scenarios = @(
        "k4_p1_tm1",
        "k4_p2_tm1",
        "k4_p4_tm1",
        "k4_p2_tm2",
        "k4_p4_tm2"
    ),

    [switch]$NoPreprocess,

    [switch]$NoMerge,

    [ValidateRange(0, 3600)]
    [int]$PostDrainSeconds = 60,

    [ValidateRange(1, 7200)]
    [int]$MergeTimeoutSeconds = 900
)

$ErrorActionPreference = "Stop"
$KafkaPartitions = 4
$KafkaReplicationFactor = 2

$ProjectRoot = Split-Path $PSScriptRoot -Parent
Set-Location $ProjectRoot

if (-not (Test-Path ".\docker-compose.yml")) {
    throw "docker-compose.yml non trovato in $ProjectRoot"
}

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock]$Command
    )

    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "Comando fallito con exit code $LASTEXITCODE."
    }
}

function Wait-ContainerHealthy {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,

        [int]$Attempts = 60
    )

    for ($Attempt = 1; $Attempt -le $Attempts; $Attempt++) {
        $Status = docker inspect --format "{{.State.Health.Status}}" $Name 2>$null
        if ($LASTEXITCODE -eq 0 -and $Status -eq "healthy") {
            return
        }
        Start-Sleep -Seconds 2
    }

    throw "Container '$Name' non healthy dopo $Attempts tentativi."
}

function Wait-KafkaApi {
    param([int]$Attempts = 60)

    for ($Attempt = 1; $Attempt -le $Attempts; $Attempt++) {
        docker exec kafka /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server kafka:9092 --list *> $null
        if ($LASTEXITCODE -eq 0) {
            return
        }
        Start-Sleep -Seconds 2
    }

    throw "Kafka topic API non pronta dopo $Attempts tentativi."
}

function Assert-NoRunningProducer {
    $RunningProducers = @(
        docker ps --filter "name=producer" --format "{{.Names}}" |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )

    if ($LASTEXITCODE -ne 0) {
        throw "Impossibile controllare eventuali producer gia' in esecuzione."
    }

    if ($RunningProducers.Count -gt 0) {
        throw (
            "Producer gia' in esecuzione: " + ($RunningProducers -join ", ") +
            ". Ferma la run precedente prima di lanciare la suite."
        )
    }
}

function Get-ScenarioConfig {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Scenario
    )

    $ConfigPath = ".\config\experiments\$Scenario.yml"
    if (-not (Test-Path $ConfigPath)) {
        throw "Config scenario non trovato: $ConfigPath"
    }

    $PreviousConfigPath = $env:CONFIG_PATH
    try {
        $env:CONFIG_PATH = $ConfigPath
        $PythonCode = @'
import json
from common.config import load_config

cfg = load_config()
deployment = cfg.get('deployment', {}).get('flink', {})
print(json.dumps({
    'parallelism': int(cfg['flink']['parallelism']),
    'taskmanagers': int(deployment.get('taskmanagers', 1)),
    'slots_per_taskmanager': int(deployment.get('slots_per_taskmanager', 4)),
}))
'@
        $RawConfig = python -c $PythonCode
        if ($LASTEXITCODE -ne 0) {
            throw "Impossibile leggere la config $ConfigPath"
        }
        $Config = $RawConfig | ConvertFrom-Json
    }
    finally {
        if ($null -eq $PreviousConfigPath) {
            Remove-Item Env:\CONFIG_PATH -ErrorAction SilentlyContinue
        }
        else {
            $env:CONFIG_PATH = $PreviousConfigPath
        }
    }

    $Capacity = [int]$Config.taskmanagers * [int]$Config.slots_per_taskmanager
    if ($Capacity -ne [int]$Config.parallelism) {
        throw (
            "Scenario ${Scenario} non valido per la matrice: capacity=$Capacity, " +
            "parallelism=$($Config.parallelism). Devono coincidere."
        )
    }

    return $Config
}

function Reset-FixedKafkaTopic {
    Write-Host "Reset flights: partitions=$KafkaPartitions, RF=$KafkaReplicationFactor..."

    Invoke-Checked {
        docker exec kafka /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server kafka:9092 `
            --delete --topic flights --if-exists
    }

    Start-Sleep -Seconds 3

    Invoke-Checked {
        docker exec kafka /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server kafka:9092 `
            --create --topic flights `
            --partitions $KafkaPartitions `
            --replication-factor $KafkaReplicationFactor `
            --config retention.ms=172800000 `
            --config max.message.bytes=2097152
    }
}

function Get-TopicEndOffsetTotal {
    $Offsets = @(
        docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh `
            --bootstrap-server kafka:9092 `
            --topic flights `
            --time -1
    )

    if ($LASTEXITCODE -ne 0) {
        throw "Impossibile leggere gli end offset del topic flights."
    }

    $Total = [int64]0
    foreach ($Line in $Offsets) {
        $Parts = $Line.Trim().Split(":")
        if ($Parts.Count -ge 3) {
            $Total += [int64]$Parts[-1]
        }
    }

    return $Total
}

Assert-NoRunningProducer

Write-Host ""
Write-Host "Starting Kafka and Schema Registry infrastructure..."
Invoke-Checked {
    docker compose up -d kafka kafka2 schema-registry schema-init
}
Wait-ContainerHealthy -Name "kafka"
Wait-ContainerHealthy -Name "kafka2"
Wait-ContainerHealthy -Name "schema-registry"
Wait-KafkaApi

if (-not $NoPreprocess) {
    Write-Host ""
    Write-Host "Running preprocessing once for the whole suite..."
    Invoke-Checked {
        docker compose run --rm --build `
            -e CONFIG_PATH=/config/base.yml `
            preprocess
    }
}
else {
    Write-Host "Preprocessing skipped (-NoPreprocess)."
}

$TotalRuns = $Scenarios.Count * $Queries.Count
$RunNumber = 0

foreach ($Scenario in $Scenarios) {
    $ScenarioConfig = Get-ScenarioConfig -Scenario $Scenario

    Write-Host ""
    Write-Host "################################################################"
    Write-Host "Scenario     : $Scenario"
    Write-Host "Kafka        : $KafkaPartitions partitions (fixed)"
    Write-Host "Flink        : parallelism=$($ScenarioConfig.parallelism)"
    Write-Host "Deployment   : $($ScenarioConfig.taskmanagers) TM x $($ScenarioConfig.slots_per_taskmanager) slot/TM"
    Write-Host "################################################################"

    Reset-FixedKafkaTopic

    Write-Host "Preloading the fixed Kafka backlog once for scenario $Scenario..."
    Invoke-Checked {
        docker compose run --rm --build `
            -e CONFIG_PATH=/config/experiments/$Scenario.yml `
            producer
    }

    $TopicRecords = Get-TopicEndOffsetTotal
    if ($TopicRecords -le $KafkaPartitions) {
        throw (
            "Backlog non valido per ${Scenario}: end-offset totali=$TopicRecords. " +
            "Attesi dataset completo piu' marker EOS."
        )
    }
    Write-Host "Kafka backlog ready: total end offsets=$TopicRecords."

    foreach ($CurrentQuery in $Queries) {
        $RunNumber++
        Write-Host ""
        Write-Host "================================================================"
        Write-Host "Scalability run $RunNumber/${TotalRuns}: $Scenario / $CurrentQuery"
        Write-Host "================================================================"

        $RunArgs = @{
            Exp                                    = $Scenario
            Query                                  = $CurrentQuery
            Engine                                 = "flink"
            Implementation                         = "table"
            NoResetTopic                           = $true
            NoProducer                             = $true
            NoPreprocess                           = $true
            ExpectedSourceRecords                  = $TopicRecords
            PostProducerDrainSeconds               = $PostDrainSeconds
            MergeTimeoutSeconds                    = $MergeTimeoutSeconds
        }

        if ($NoMerge) {
            $RunArgs["NoMerge"] = $true
        }

        & .\scripts\run_experiment.ps1 @RunArgs
    }
}

Write-Host ""
Write-Host "################################################################"
Write-Host "Suite completata: $TotalRuns run, Kafka sempre a 4 partizioni."
Write-Host "Metriche: Results\perf.csv"
if ($NoMerge) {
    Write-Host "Output merged: skipped (-NoMerge)"
}
else {
    Write-Host "Output merged: Results\q*_flink_table_k4_p*_tm*.csv"
}
Write-Host "################################################################"
