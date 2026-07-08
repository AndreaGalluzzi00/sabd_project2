<#
run_spark_backlog_p4n4.ps1

Runs Spark Structured Streaming on the same p4_n4 backlog precondition used by
the Flink partition-matrix comparison:
  1. start Kafka/Kafka2 and Schema Registry;
  2. recreate flights with 4 partitions and replication factor 2;
  3. preload the topic once with config/experiments/p4_n4.yml;
  4. run Spark q1/q2/q3 with -NoResetTopic so every query reads the same backlog.

Usage, from the project root:
  powershell -ExecutionPolicy Bypass -File .\scripts\run_spark_backlog_p4n4.ps1
  powershell -ExecutionPolicy Bypass -File .\scripts\run_spark_backlog_p4n4.ps1 -Queries q1,q3
#>
param(
    [ValidateSet("q1", "q2", "q3")]
    [string[]]$Queries = @("q1", "q2", "q3"),

    [string]$Experiment = "p4_n4",

    [int]$Partitions = 4,

    [int]$ReplicationFactor = 2,

    [switch]$Merge
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path $PSScriptRoot -Parent
Set-Location $ProjectRoot

if (-not (Test-Path ".\docker-compose.yml")) {
    throw "docker-compose.yml non trovato in $ProjectRoot"
}

$ConfigHost = ".\config\experiments\$Experiment.yml"
if (-not (Test-Path $ConfigHost)) {
    throw "Config esperimento non trovato: $ConfigHost"
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

        [int]$Attempts = 45
    )

    for ($Attempt = 1; $Attempt -le $Attempts; $Attempt++) {
        $Status = docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $Name 2>$null

        if ($LASTEXITCODE -eq 0 -and ($Status -eq "healthy" -or $Status -eq "running")) {
            return
        }

        Start-Sleep -Seconds 2
    }

    throw "Container '$Name' non pronto dopo $Attempts tentativi."
}


function Wait-KafkaApi {
    param([int]$Attempts = 30)

    for ($Attempt = 1; $Attempt -le $Attempts; $Attempt++) {
        docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list *> $null

        if ($LASTEXITCODE -eq 0) {
            return
        }

        Start-Sleep -Seconds 2
    }

    throw "Kafka topic API non pronta dopo $Attempts tentativi."
}


function Assert-NoRunningOneShotContainers {
    $ProducerRunning = @(
        docker ps --filter "name=producer-run" --format "{{.Names}}" |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
    if ($LASTEXITCODE -ne 0) {
        throw "Impossibile controllare eventuali producer gia' in esecuzione."
    }

    $SparkRunning = @(
        docker ps --filter "name=spark-job-" --format "{{.Names}}" |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
    if ($LASTEXITCODE -ne 0) {
        throw "Impossibile controllare eventuali Spark job gia' in esecuzione."
    }

    $Running = @($ProducerRunning + $SparkRunning)

    if ($Running.Count -gt 0) {
        throw (
            "Container gia' in esecuzione: " +
            ($Running -join ", ") +
            ". Ferma la run precedente prima di lanciare il confronto Spark."
        )
    }
}


function Start-SparkComparisonInfrastructure {
    Write-Host ""
    Write-Host "Starting Kafka, Schema Registry and Spark prerequisites..."

    Invoke-Checked {
        docker compose up -d `
            kafka kafka2 schema-registry schema-init
    }

    Wait-ContainerHealthy -Name "kafka"
    Wait-ContainerHealthy -Name "kafka2"
    Wait-ContainerHealthy -Name "schema-registry"
    Wait-KafkaApi
}


function Reset-FlightsTopic {
    Write-Host ""
    Write-Host "Reset Kafka topic flights (partitions=$Partitions, RF=$ReplicationFactor)..."

    Invoke-Checked {
        docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 `
            --delete --topic flights --if-exists
    }

    Start-Sleep -Seconds 5

    Invoke-Checked {
        docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 `
            --create `
            --topic flights `
            --partitions $Partitions `
            --replication-factor $ReplicationFactor `
            --config retention.ms=172800000 `
            --config max.message.bytes=2097152
    }

    Invoke-Checked {
        docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 `
            --describe --topic flights
    }
}


function Preload-Backlog {
    Write-Host ""
    Write-Host "Preloading Kafka backlog with $Experiment..."

    Invoke-Checked {
        docker compose run --rm -e CONFIG_PATH=/config/experiments/$Experiment.yml producer
    }
}


function Run-SparkQuery {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("q1", "q2", "q3")]
        [string]$Query
    )

    Write-Host ""
    Write-Host "########## Spark $Query on preloaded $Experiment backlog ##########"

    $RunArgs = @{
        Exp = $Experiment
        Query = $Query
        Engine = "spark"
        NoResetTopic = $true
        NoPreprocess = $true
    }

    if (-not $Merge) {
        $RunArgs.NoMerge = $true
    }

    Invoke-Checked {
        & .\scripts\run_experiment.ps1 @RunArgs
    }
}


Assert-NoRunningOneShotContainers
Start-SparkComparisonInfrastructure
Reset-FlightsTopic
Preload-Backlog

foreach ($Query in $Queries) {
    Run-SparkQuery -Query $Query
}

Write-Host ""
Write-Host "########## SPARK BACKLOG COMPARISON COMPLETED ##########"
Write-Host "Results: Results\perf.csv (engine=spark, experiment=$Experiment)."
