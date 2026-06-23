<#
USAGE - run_experiment.ps1

Eseguire i comandi dalla root del progetto, cioè dalla cartella in cui si trova docker-compose.yml.

Prima esecuzione:
    docker compose build
    docker compose up -d

    docker exec flink-jobmanager flink list -r
    docker exec flink-jobmanager flink cancel <JOB_ID>

Esecuzione con config base:
    .\scripts\run_experiment.ps1

Esecuzione esperimento 1:
    .\scripts\run_experiment.ps1 -e 01_baseline

Esecuzione esperimento 2:
    .\scripts\run_experiment.ps1 -e 02_ooo_safe

Esecuzione esperimento 3:
    .\scripts\run_experiment.ps1 -e 03_ooo_late_loss

Esecuzione esperimento 4:
    .\scripts\run_experiment.ps1 -e 04_ooo_uniform_late_loss

Esecuzione esperimento 5 (watermark safe, completezza ~100%):
    .\scripts\run_experiment.ps1 -e 05_wm_safe

Esecuzione esperimento 6 (watermark aggressive, perdita attesa ~12.6%):
    .\scripts\run_experiment.ps1 -e 06_wm_aggressive

Se il preprocessing è già stato eseguito:
    .\scripts\run_experiment.ps1 -e 02_ooo_safe -NoPreprocess

Per non cancellare i risultati precedenti:
    .\scripts\run_experiment.ps1 -e 02_ooo_safe -NoPreprocess -NoCleanResults

Per cambiare il tempo di attesa prima del merge automatico:
    .\scripts\run_experiment.ps1 -e 02_ooo_safe -NoPreprocess -MergeDelaySeconds 35

Per disattivare il merge automatico:
    .\scripts\run_experiment.ps1 -e 02_ooo_safe -NoPreprocess -NoMerge

Merge manuale:
    python .\scripts\merge_q1.py --exp 02_ooo_safe

Esecuzione consigliata di tutti gli esperimenti:
    .\scripts\run_experiment.ps1 -e 01_baseline
    .\scripts\run_experiment.ps1 -e 02_ooo_safe -NoPreprocess
    .\scripts\run_experiment.ps1 -e 03_ooo_late_loss -NoPreprocess
    .\scripts\run_experiment.ps1 -e 04_ooo_uniform_late_loss -NoPreprocess
    .\scripts\run_experiment.ps1 -e 05_wm_safe -NoPreprocess
    .\scripts\run_experiment.ps1 -e 06_wm_aggressive -NoPreprocess

Parametri disponibili:
    -e / -Exp              Nome dell'esperimento dentro config/experiments.
    -NoPreprocess          Salta il preprocessing.
    -NoResetTopic          Non cancella e non ricrea il topic Kafka flights.
    -NoCleanResults        Non cancella la cartella dei part file prima del run.
    -NoMerge               Non esegue il merge automatico.
    -MergeDelaySeconds     Numero di secondi da attendere prima del merge. Default: 25.
#>

param(
    [Alias("e")]
    [string]$Exp,

    [switch]$NoPreprocess,
    [switch]$NoResetTopic,
    [switch]$NoCleanResults,
    [switch]$NoMerge,

    [int]$MergeDelaySeconds = 25
)

$ErrorActionPreference = "Stop"

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

function Assert-ProjectRoot {
    if (-not (Test-Path ".\docker-compose.yml")) {
        throw "Devi eseguire lo script dalla root del progetto, dove si trova docker-compose.yml."
    }
}

function Get-Q1ResultsHostPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ConfigPathHost
    )

    $PreviousConfigPath = $env:CONFIG_PATH

    try {
        $env:CONFIG_PATH = $ConfigPathHost

        $PathValue = python -c "from pathlib import Path; from common.config import load_config; print(Path(load_config()['paths']['q1_results_host_path']))"

        if ($LASTEXITCODE -ne 0) {
            throw "Unable to read q1_results_host_path from config: $ConfigPathHost"
        }

        return $PathValue.Trim()
    }
    finally {
        if ($null -eq $PreviousConfigPath) {
            Remove-Item Env:\CONFIG_PATH -ErrorAction SilentlyContinue
        }
        else {
            $env:CONFIG_PATH = $PreviousConfigPath
        }
    }
}

function Initialize-Q1ResultsDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ResultsHostPath,

        [Parameter(Mandatory = $true)]
        [bool]$Clean
    )

    Write-Host ""
    Write-Host "Q1 host results directory:"
    Write-Host $ResultsHostPath

    if ($Clean -and (Test-Path $ResultsHostPath)) {
        Write-Host "Cleaning previous Q1 part files..."
        Remove-Item -Recurse -Force $ResultsHostPath
    }

    Write-Host "Ensuring Q1 host results directory exists..."
    New-Item -ItemType Directory -Force -Path $ResultsHostPath | Out-Null
}

Assert-ProjectRoot

if ($MergeDelaySeconds -lt 0) {
    throw "MergeDelaySeconds non può essere negativo."
}

if ([string]::IsNullOrWhiteSpace($Exp)) {
    $CfgHost = "config/base.yml"
    $CfgContainer = "/config/base.yml"
    $Label = "base"
    $MergeArgs = @()
}
else {
    $CfgHost = "config/experiments/$Exp.yml"
    $ExperimentHostPath = ".\config\experiments\$Exp.yml"

    if (-not (Test-Path $ExperimentHostPath)) {
        throw "Config esperimento non trovato: $ExperimentHostPath"
    }

    $CfgContainer = "/config/experiments/$Exp.yml"
    $Label = $Exp
    $MergeArgs = @("--exp", $Exp)
}

$Q1ResultsHostPath = Get-Q1ResultsHostPath -ConfigPathHost $CfgHost

Initialize-Q1ResultsDirectory `
    -ResultsHostPath $Q1ResultsHostPath `
    -Clean:(-not $NoCleanResults)

Write-Host ""
Write-Host "========================================"
Write-Host "Running Q1 experiment: $Label"
Write-Host "Config host         : $CfgHost"
Write-Host "Config inside Docker: $CfgContainer"
Write-Host "========================================"
Write-Host ""

if (-not $NoResetTopic) {
    Write-Host "Reset Kafka topic flights..."

    Invoke-Checked {
        docker exec kafka /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server kafka:9092 `
            --delete `
            --topic flights `
            --if-exists
    }

    Start-Sleep -Seconds 3

    Invoke-Checked {
        docker compose run --rm kafka-init
    }
}
else {
    Write-Host "Kafka topic reset skipped."
}

if (-not $NoPreprocess) {
    Write-Host ""
    Write-Host "Running preprocessing with base config..."

    Invoke-Checked {
        docker compose run --rm `
            -e CONFIG_PATH=/config/base.yml `
            preprocess
    }
}
else {
    Write-Host ""
    Write-Host "Preprocessing skipped."
}

Write-Host ""
Write-Host "Submitting Flink Q1 job..."

Invoke-Checked {
    docker compose run --rm `
        -e CONFIG_PATH=$CfgContainer `
        flink-job-q1
}

Write-Host ""
Write-Host "Running producer..."

Invoke-Checked {
    docker compose run --rm `
        -e CONFIG_PATH=$CfgContainer `
        producer
}

Write-Host ""
Write-Host "Flink running jobs:"

Invoke-Checked {
    docker exec flink-jobmanager flink list -r
}

if (-not $NoMerge) {
    Write-Host ""
    Write-Host "Waiting $MergeDelaySeconds seconds before merge..."
    Start-Sleep -Seconds $MergeDelaySeconds

    Write-Host ""
    Write-Host "Merging Q1 results..."

    Invoke-Checked {
        python .\scripts\merge_q1.py @MergeArgs
    }
}
else {
    Write-Host ""
    Write-Host "Merge skipped."
}

Write-Host ""
Write-Host "Done: $Label"