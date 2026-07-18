<#
run_completeness_control.ps1

Run controlled completeness experiments with:
  - Kafka topic flights: 1 partition, replication factor 1
  - Flink parallelism: 1
  - Flink Table optimizer aggregate phase: ONE_PHASE

The script creates derived experiment configs named:
  <source_exp>_ctrl_p1n1_onephase

This keeps control outputs separate from the normal completeness runs.
#>
param(
    [ValidateSet("q1", "q2", "q3", "all")]
    [string]$Query = "all",

    [string[]]$Experiments = @(),

    [int]$KafkaPartitions = 1,

    [int]$KafkaReplicationFactor = 1,

    [switch]$NoPreprocess,

    [switch]$NoMerge,

    [int]$PostProducerDrainSeconds = 180,

    [int]$MergeTimeoutSeconds = 900
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path $PSScriptRoot -Parent
Set-Location $ProjectRoot

if (-not (Test-Path ".\docker-compose.yml")) {
    throw "docker-compose.yml non trovato in $ProjectRoot"
}

if ($KafkaPartitions -le 0) {
    throw "KafkaPartitions deve essere maggiore di zero."
}

if ($KafkaReplicationFactor -le 0) {
    throw "KafkaReplicationFactor deve essere maggiore di zero."
}

if ($PostProducerDrainSeconds -lt 0) {
    throw "PostProducerDrainSeconds deve essere 0 oppure un intero positivo."
}

if ($Query -eq "all" -and $Experiments.Count -gt 0) {
    throw "Usa -Experiments solo con una query specifica, non con -Query all."
}

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock]$Command
    )

    & $Command

    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code $LASTEXITCODE"
    }
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

function Start-ControlInfrastructure {
    Write-Host ""
    Write-Host "Starting Kafka and Schema Registry infrastructure..."

    Invoke-Checked {
        docker compose up -d `
            kafka kafka2 schema-registry schema-init
    }

    Wait-KafkaApi
}

function Reset-FlightsTopic {
    param(
        [Parameter(Mandatory = $true)]
        [int]$Partitions,

        [Parameter(Mandatory = $true)]
        [int]$ReplicationFactor
    )

    Write-Host ""
    Write-Host "Reset Kafka topic flights (partitions=$Partitions, RF=$ReplicationFactor)..."

    Invoke-Checked {
        docker exec kafka /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server kafka:9092 `
            --delete `
            --topic flights `
            --if-exists
    }

    Start-Sleep -Seconds 3

    Invoke-Checked {
        docker exec kafka /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server kafka:9092 `
            --create `
            --topic flights `
            --partitions $Partitions `
            --replication-factor $ReplicationFactor `
            --config retention.ms=172800000 `
            --config max.message.bytes=2097152
    }
}

function New-ControlExperimentConfig {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SourceExperiment
    )

    $SourcePath = ".\config\experiments\$SourceExperiment.yml"
    if (-not (Test-Path $SourcePath)) {
        throw "Config esperimento non trovato: $SourcePath"
    }

    $ControlExperiment = "${SourceExperiment}_ctrl_p1n1_onephase"
    $ControlPath = ".\config\experiments\$ControlExperiment.yml"

    $Content = @"
extends: "$SourceExperiment.yml"

experiment:
  name: "$ControlExperiment"
  output_dir_host: "Results/experiments"

flink:
  parallelism: 1
  table_optimizer_agg_phase_strategy: "ONE_PHASE"

deployment:
  flink:
    taskmanagers: 1
    slots_per_taskmanager: 1
"@

    Set-Content -Path $ControlPath -Value $Content -Encoding UTF8

    return $ControlExperiment
}

$DefaultExperiments = @{
    q1 = @(
        "1h_wm_uniform_d0",
        "1h_wm_uniform_d1800",
        "1h_wm_uniform_d3600"
    )
    q2 = @(
        "1h_wm_uniform_d0",
        "1h_wm_uniform_d1800",
        "1h_wm_uniform_d3600",
        "6h_wm_uniform_d0",
        "6h_wm_uniform_d10800",
        "6h_wm_uniform_d21600"
    )
    q3 = @(
        "1d_wm_uniform_d0",
        "1d_wm_uniform_d43200",
        "1d_wm_uniform_d86400",
        "7d_wm_uniform_d0",
        "7d_wm_uniform_d302400",
        "7d_wm_uniform_d604800"
    )
}

$Queries = if ($Query -eq "all") {
    @("q1", "q2", "q3")
}
else {
    @($Query)
}

Start-ControlInfrastructure

foreach ($CurrentQuery in $Queries) {
    $ExperimentList = if ($Experiments.Count -gt 0) {
        $Experiments
    }
    else {
        $DefaultExperiments[$CurrentQuery]
    }

    foreach ($Experiment in $ExperimentList) {
        $ControlExperiment = New-ControlExperimentConfig -SourceExperiment $Experiment

        Write-Host ""
        Write-Host "============================================================"
        Write-Host "Completeness control run"
        Write-Host "Query        : $CurrentQuery"
        Write-Host "Source exp   : $Experiment"
        Write-Host "Control exp  : $ControlExperiment"
        Write-Host "Kafka topic  : partitions=$KafkaPartitions RF=$KafkaReplicationFactor"
        Write-Host "Flink config : parallelism=1 agg=ONE_PHASE"
        Write-Host "Drain wait   : $PostProducerDrainSeconds s after producer"
        Write-Host "============================================================"

        Reset-FlightsTopic `
            -Partitions $KafkaPartitions `
            -ReplicationFactor $KafkaReplicationFactor

        # Perf monitor stays ENABLED here (no NoPerf) so every completeness /
        # late-drop run also appends a perf.csv row carrying elapsed_ms, giving a
        # processing-time reading alongside the late-drop count for each
        # watermark-delay configuration. The monitor runs to job drain BEFORE the
        # late-drop metric is read via REST, so the two collections don't clash.
        $RunArgs = @{
            Exp                       = $ControlExperiment
            Query                     = $CurrentQuery
            Engine                    = "flink"
            Implementation            = "table"
            NoResetTopic              = $true
            FlinkParallelismOverride  = 1
            FlinkAggPhaseStrategy     = "ONE_PHASE"
            PostProducerDrainSeconds  = $PostProducerDrainSeconds
            MergeTimeoutSeconds       = $MergeTimeoutSeconds
        }

        if ($NoPreprocess) {
            $RunArgs["NoPreprocess"] = $true
        }

        if ($NoMerge) {
            $RunArgs["NoMerge"] = $true
        }

        Invoke-Checked {
            & .\scripts\run_experiment.ps1 @RunArgs
        }
    }
}

Write-Host ""
Write-Host "Completeness control runs completed."
