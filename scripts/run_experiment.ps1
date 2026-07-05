<#
run_experiment.ps1

Runner unico per eseguire una query (q1/q2/q3) su Flink o Spark, con una
config di esperimento opzionale. Va lanciato dalla root del progetto, cioe'
dalla cartella che contiene docker-compose.yml.

USO RAPIDO
    # Default: q1 + flink + table + config/base.yml
    .\scripts\run_experiment.ps1

    # Stesso scenario sperimentale, implementazioni diverse
    .\scripts\run_experiment.ps1 -e 01_baseline -Query q1 -Engine flink -Implementation table
    .\scripts\run_experiment.ps1 -e 01_baseline -Query q1 -Engine flink -Implementation datastream
    .\scripts\run_experiment.ps1 -e 01_baseline -Query q1 -Engine spark

    # Q2 / Q3
    .\scripts\run_experiment.ps1 -Query q2 -Engine flink
    .\scripts\run_experiment.ps1 -Query q2 -Engine flink -Implementation datastream
    .\scripts\run_experiment.ps1 -Query q2 -Engine spark
    .\scripts\run_experiment.ps1 -Query q3 -Engine flink
    .\scripts\run_experiment.ps1 -Query q3 -Engine spark

COMBINAZIONI SUPPORTATE
    Flink:
        q1 table, q1 datastream
        q2 table, q2 datastream
        q3 table, q3 datastream

    Spark:
        q1 structured
        q2 structured
        q3 structured

    Nota: se -Implementation non viene passato, usa table per Flink e
    structured per Spark.

ESPERIMENTI
    Gli esperimenti stanno in config/experiments/*.yml e descrivono lo
    scenario dati/watermark/producer, non l'implementazione.

    Esempi:
        .\scripts\run_experiment.ps1 -e 01_baseline
        .\scripts\run_experiment.ps1 -e 02_ooo_safe -NoPreprocess
        .\scripts\run_experiment.ps1 -e 06_wm_aggressive -Query q1 -Engine spark

RISULTATI
    I part-file vengono puliti a inizio run, salvo -NoCleanResults.
    I CSV finali vengono creati dagli script di merge.

    Esempio per 01_baseline:
        Results/experiments/01_baseline/flink/table/q1/...
        Results/experiments/01_baseline/flink/datastream/q1/...
        Results/experiments/01_baseline/spark/structured/q1/...

OPZIONI UTILI
    -NoPreprocess          Salta il preprocessing se e' gia' stato fatto.
    -NoResetTopic          Non resetta il topic Kafka flights.
    -NoCleanResults        Non cancella i part-file precedenti.
    -NoMerge               Non crea il CSV finale.
    -MergeTimeoutSeconds   Timeout per attendere part-file stabili. Default: 180.
    -SparkTimeoutSeconds   Timeout per attendere la fine di Spark. Default: 1200.
    -KeepFlinkJob          Non cancella il job Flink a fine run.

DASHBOARD
    Le dashboard sono supportate solo per:
        -Query q1 -Engine flink -Implementation table

    Esempi:
        .\scripts\run_experiment.ps1 -e 05_wm_safe -FullFlow
        .\scripts\run_experiment.ps1 -e 05_wm_safe -DashboardInflux
        .\scripts\run_experiment.ps1 -e 05_wm_safe -DashboardTimescale

RESET COMPLETO DOCKER
    docker compose down -v --remove-orphans
    docker compose up -d --build
#>
param(
    [Alias("e")]
    [string]$Exp,

    [ValidateSet("q1", "q2", "q3")]
    [string]$Query = "q1",

    [ValidateSet("flink", "spark")]
    [string]$Engine = "flink",

    [ValidateSet("", "table", "datastream", "structured")]
    [string]$Implementation = "",

    [switch]$NoPreprocess,
    [switch]$NoResetTopic,
    [switch]$NoCleanResults,
    [switch]$NoMerge,

    [switch]$FullFlow,
    [switch]$DashboardInflux,
    [switch]$DashboardTimescale,
    [switch]$NoCleanDashboard,
    [switch]$KeepFlinkJob,
    [switch]$NoPerf,

    [int]$MergeTimeoutSeconds = 180,

    [int]$SparkTimeoutSeconds = 1200
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

function Get-ResultsHostPaths {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ConfigPathHost,

        [Parameter(Mandatory = $true)]
        [string[]]$PathKeys
    )

    $PreviousConfigPath = $env:CONFIG_PATH

    try {
        $env:CONFIG_PATH = $ConfigPathHost

        $JoinedKeys = $PathKeys -join ","
        $PathValue = python -c "from pathlib import Path; from common.config import load_config; paths=load_config()['paths']; print('\n'.join(str(Path(paths[k])) for k in '$JoinedKeys'.split(',')))"

        if ($LASTEXITCODE -ne 0) {
            throw "Unable to read result path(s) from config: $ConfigPathHost"
        }

        return @($PathValue -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
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

function Initialize-ResultsDirectories {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$ResultsHostPaths,

        [Parameter(Mandatory = $true)]
        [bool]$Clean
    )

    Write-Host ""
    Write-Host "Host results director$(if ($ResultsHostPaths.Count -eq 1) { 'y' } else { 'ies' }):"

    foreach ($ResultsHostPath in $ResultsHostPaths) {
        Write-Host $ResultsHostPath

        if ($Clean -and (Test-Path $ResultsHostPath)) {
            Write-Host "Cleaning previous part files..."
            Remove-Item -Recurse -Force $ResultsHostPath
        }

        Write-Host "Ensuring host results directory exists..."
        New-Item -ItemType Directory -Force -Path $ResultsHostPath | Out-Null
    }
}

function New-ExperimentRuntimeConfig {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ConfigPathHost,

        [Parameter(Mandatory = $true)]
        [string]$Label,

        [Parameter(Mandatory = $true)]
        [bool]$EnableInflux,

        [Parameter(Mandatory = $true)]
        [bool]$EnableTimescale
    )

    $RuntimeDir = "config/runtime"
    New-Item -ItemType Directory -Force -Path $RuntimeDir | Out-Null

    $SafeLabel = $Label -replace "[^A-Za-z0-9_.-]", "_"
    $RunId = "$(Get-Date -Format 'yyyyMMddHHmmss')-$PID"
    $RuntimeFileName = "$SafeLabel.$RunId.runtime.yml"
    $RuntimeHostPath = Join-Path $RuntimeDir $RuntimeFileName
    $FlinkConsumerGroup = "flink-flight-analysis-$SafeLabel-$RunId"
    $SparkConsumerGroup = "spark-flight-analysis-$SafeLabel-$RunId"

    $BaseFileName = Split-Path $ConfigPathHost -Leaf
    if ($ConfigPathHost -like "config/experiments/*") {
        $ExtendsPath = "../experiments/$BaseFileName"
    }
    else {
        $ExtendsPath = "../base.yml"
    }

    $InfluxEnabled = if ($EnableInflux) { "true" } else { "false" }
    $TimescaleEnabled = if ($EnableTimescale) { "true" } else { "false" }

    $RuntimeConfig = @"
extends: "$ExtendsPath"

flink:
  consumer_group: "$FlinkConsumerGroup"

spark:
  consumer_group: "$SparkConsumerGroup"

dashboard:
  influx:
    enabled: $InfluxEnabled
  timescale:
    enabled: $TimescaleEnabled
"@

    Set-Content -Path $RuntimeHostPath -Value $RuntimeConfig -Encoding UTF8

    return @{
        HostPath = $RuntimeHostPath
        ContainerPath = "/config/runtime/$RuntimeFileName"
        FlinkConsumerGroup = $FlinkConsumerGroup
        SparkConsumerGroup = $SparkConsumerGroup
    }
}

function Get-DashboardProfileArgs {
    param(
        [Parameter(Mandatory = $true)]
        [bool]$EnableInflux,

        [Parameter(Mandatory = $true)]
        [bool]$EnableTimescale
    )

    $ProfileArgs = @()

    if ($EnableInflux) {
        $ProfileArgs += @("--profile", "dashboard-influx")
    }

    if ($EnableTimescale) {
        $ProfileArgs += @("--profile", "dashboard-timescale")
    }

    return $ProfileArgs
}

function Start-DashboardStack {
    param(
        [Parameter(Mandatory = $true)]
        [bool]$EnableInflux,

        [Parameter(Mandatory = $true)]
        [bool]$EnableTimescale
    )

    $ProfileArgs = Get-DashboardProfileArgs `
        -EnableInflux $EnableInflux `
        -EnableTimescale $EnableTimescale

    Write-Host ""
    Write-Host "Starting infrastructure/dashboard profiles..."
    Write-Host ("Profiles: " + ($ProfileArgs -join " "))

    Invoke-Checked {
        docker compose @ProfileArgs up -d
    }
}

function Test-DockerContainerRunning {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $Names = @(docker ps --format "{{.Names}}")

    if ($LASTEXITCODE -ne 0) {
        throw "Unable to inspect Docker containers."
    }

    return $Names -contains $Name
}

function Get-RunningFlinkJobIds {
    if (-not (Test-DockerContainerRunning -Name "flink-jobmanager")) {
        Write-Host "Flink JobManager not running; no existing jobs to cancel."
        return @()
    }

    $PreviousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $Output = @(docker exec flink-jobmanager flink list -r 2>&1)
        $ExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $PreviousErrorActionPreference
    }

    if ($ExitCode -ne 0) {
        Write-Warning "Unable to list running Flink jobs; continuing. Output: $($Output -join ' ')"
        return @()
    }

    $Text = $Output -join "`n"
    $Matches = [regex]::Matches($Text, "\b[a-f0-9]{32}\b")

    return @($Matches | ForEach-Object { $_.Value } | Select-Object -Unique)
}

function Clear-RunningFlinkJobs {
    $JobIds = @(Get-RunningFlinkJobIds)

    if ($JobIds.Count -eq 0) {
        Write-Host "No running Flink jobs to cancel."
        return
    }

    Write-Host "Cancelling existing Flink job(s): $($JobIds -join ', ')"

    foreach ($JobId in $JobIds) {
        Invoke-Checked {
            docker exec flink-jobmanager flink cancel $JobId
        }
    }

    Start-Sleep -Seconds 5
}

function Reset-KafkaTopic {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Topic
    )

    Invoke-Checked {
        docker exec kafka /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server kafka:9092 `
            --delete `
            --topic $Topic `
            --if-exists
    }
}

function Ensure-KafkaTopic {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Topic,

        [int]$Partitions = 4,

        [int]$ReplicationFactor = 1
    )

    Invoke-Checked {
        docker exec kafka /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server kafka:9092 `
            --create `
            --if-not-exists `
            --topic $Topic `
            --partitions $Partitions `
            --replication-factor $ReplicationFactor
    }
}

function Wait-TimescaleDb {
    for ($Attempt = 1; $Attempt -le 30; $Attempt++) {
        docker exec sabd2-timescaledb pg_isready -U sabd -d sabd | Out-Null

        if ($LASTEXITCODE -eq 0) {
            return
        }

        Start-Sleep -Seconds 2
    }

    throw "TimescaleDB non pronto dopo 60 secondi."
}

function Clear-TimescaleQ1Results {
    Write-Host ""
    Write-Host "Cleaning TimescaleDB q1_results..."

    Wait-TimescaleDb

    Invoke-Checked {
        docker exec sabd2-timescaledb psql `
            -U sabd `
            -d sabd `
            -c "TRUNCATE TABLE q1_results;"
    }
}

function Resolve-Implementation {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("flink", "spark")]
        [string]$Engine,

        [Parameter(Mandatory = $true)]
        [string]$Implementation
    )

    if ([string]::IsNullOrWhiteSpace($Implementation)) {
        if ($Engine -eq "spark") {
            return "structured"
        }

        return "table"
    }

    return $Implementation
}

function Get-RunSpec {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("q1", "q2", "q3")]
        [string]$Query,

        [Parameter(Mandatory = $true)]
        [ValidateSet("flink", "spark")]
        [string]$Engine,

        [Parameter(Mandatory = $true)]
        [string]$Implementation
    )

    if ($Engine -eq "spark") {
        if ($Implementation -ne "structured") {
            throw "Spark supporta solo -Implementation structured."
        }

        $MergeScripts = @{
            q1 = ".\scripts\merge_spark_q1.py"
            q2 = ".\scripts\merge_spark_q2.py"
            q3 = ".\scripts\merge_spark_q3.py"
        }

        $PathKeys = @{
            q1 = @("spark_q1_results_host_path")
            q2 = @(
                "spark_q2_results_host_path_1h",
                "spark_q2_results_host_path_6h",
                "spark_q2_results_host_path_global"
            )
            q3 = @(
                "spark_q3_results_host_path_1d",
                "spark_q3_results_host_path_7d",
                "spark_q3_results_host_path_global"
            )
        }

        return @{
            Service = "spark-job-$Query"
            Container = "spark-job-$Query"
            MergeScript = $MergeScripts[$Query]
            PathKeys = $PathKeys[$Query]
        }
    }

    if ($Implementation -notin @("table", "datastream")) {
        throw "Flink supporta -Implementation table oppure datastream."
    }

    if ($Implementation -eq "datastream") {
        $MergeScripts = @{
            q1 = ".\scripts\merge_q1_ds.py"
            q2 = ".\scripts\merge_q2_ds.py"
            q3 = ".\scripts\merge_q3_ds.py"
        }

        $PathKeys = @{
            q1 = @("q1_ds_results_host_path")
            q2 = @(
                "q2_ds_results_host_path_1h",
                "q2_ds_results_host_path_6h",
                "q2_ds_results_host_path_global"
            )
            q3 = @(
                "q3_ds_results_host_path_1d",
                "q3_ds_results_host_path_7d",
                "q3_ds_results_host_path_global"
            )
        }

        return @{
            Service = "flink-job-$Query-ds"
            Container = $null
            MergeScript = $MergeScripts[$Query]
            PathKeys = $PathKeys[$Query]
        }
    }

    $MergeScripts = @{
        q1 = ".\scripts\merge_q1.py"
        q2 = ".\scripts\merge_q2.py"
        q3 = ".\scripts\merge_q3.py"
    }

    $PathKeys = @{
        q1 = @("q1_results_host_path")
        q2 = @(
            "q2_results_host_path_1h",
            "q2_results_host_path_6h",
            "q2_results_host_path_global"
        )
        q3 = @(
            "q3_results_host_path_1d",
            "q3_results_host_path_7d",
            "q3_results_host_path_global"
        )
    }

    return @{
        Service = "flink-job-$Query"
        Container = $null
        MergeScript = $MergeScripts[$Query]
        PathKeys = $PathKeys[$Query]
    }
}

function Start-SparkJob {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ConfigPathContainer,

        [Parameter(Mandatory = $true)]
        [string]$Service
    )

    $PreviousConfigPath = $env:SABD_CONFIG_PATH

    try {
        $env:SABD_CONFIG_PATH = $ConfigPathContainer

        Invoke-Checked {
            docker compose --profile manual up -d --build --force-recreate $Service
        }
    }
    finally {
        if ($null -eq $PreviousConfigPath) {
            Remove-Item Env:\SABD_CONFIG_PATH -ErrorAction SilentlyContinue
        }
        else {
            $env:SABD_CONFIG_PATH = $PreviousConfigPath
        }
    }
}

function Wait-SparkJob {
    param(
        [Parameter(Mandatory = $true)]
        [int]$TimeoutSeconds,

        [Parameter(Mandatory = $true)]
        [string]$Container
    )

    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    while ((Get-Date) -lt $Deadline) {
        $State = docker inspect -f "{{.State.Status}} {{.State.ExitCode}}" $Container

        if ($LASTEXITCODE -ne 0) {
            throw "Unable to inspect $Container."
        }

        $Parts = $State.Trim().Split(" ", [System.StringSplitOptions]::RemoveEmptyEntries)
        $Status = $Parts[0]
        $ExitCode = [int]$Parts[1]

        if ($Status -eq "exited") {
            if ($ExitCode -ne 0) {
                throw "Spark job $Container failed with exit code $ExitCode. Check logs with: docker logs $Container"
            }

            Write-Host "Spark job $Container completed successfully."
            return
        }

        if ($Status -eq "running") {
            Start-Sleep -Seconds 5
            continue
        }

        throw "Unexpected $Container status: $State"
    }

    throw "Spark job $Container did not finish within $TimeoutSeconds seconds."
}

Assert-ProjectRoot

if ($MergeTimeoutSeconds -le 0) {
    throw "MergeTimeoutSeconds deve essere maggiore di zero."
}

if ($SparkTimeoutSeconds -le 0) {
    throw "SparkTimeoutSeconds deve essere maggiore di zero."
}

$Implementation = Resolve-Implementation `
    -Engine $Engine `
    -Implementation $Implementation

$RunSpec = Get-RunSpec `
    -Query $Query `
    -Engine $Engine `
    -Implementation $Implementation

$EnableInflux = [bool]($FullFlow -or $DashboardInflux)
$EnableTimescale = [bool]($FullFlow -or $DashboardTimescale)
$DashboardEnabled = [bool]($EnableInflux -or $EnableTimescale)

if (
    $DashboardEnabled `
    -and (
        $Query -ne "q1" `
        -or $Engine -ne "flink" `
        -or $Implementation -ne "table"
    )
) {
    throw "Le dashboard sono supportate da questo runner solo per -Query q1 -Engine flink -Implementation table."
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

$RuntimeLabel = "$Label-$Query-$Engine-$Implementation"

$RuntimeCfg = New-ExperimentRuntimeConfig `
    -ConfigPathHost $CfgHost `
    -Label $RuntimeLabel `
    -EnableInflux $EnableInflux `
    -EnableTimescale $EnableTimescale

$SubmitCfgHost = $RuntimeCfg.HostPath
$SubmitCfgContainer = $RuntimeCfg.ContainerPath
$RuntimeConsumerGroup = if ($Engine -eq "spark") {
    $RuntimeCfg.SparkConsumerGroup
}
else {
    $RuntimeCfg.FlinkConsumerGroup
}

$ResultsHostPaths = Get-ResultsHostPaths `
    -ConfigPathHost $CfgHost `
    -PathKeys $RunSpec.PathKeys

Initialize-ResultsDirectories `
    -ResultsHostPaths $ResultsHostPaths `
    -Clean:(-not $NoCleanResults)

Write-Host ""
Write-Host "========================================"
Write-Host "Running experiment   : $Label"
Write-Host "Query               : $Query"
Write-Host "Engine              : $Engine"
Write-Host "Implementation      : $Implementation"
Write-Host "Config host         : $CfgHost"
Write-Host "Submit config host  : $SubmitCfgHost"
Write-Host "Config inside Docker: $SubmitCfgContainer"
Write-Host "Runtime consumer    : $RuntimeConsumerGroup"
if ($DashboardEnabled) {
    Write-Host "Dashboard InfluxDB  : $EnableInflux"
    Write-Host "Dashboard Timescale : $EnableTimescale"
}
else {
    Write-Host "Dashboard sinks     : disabled"
}
Write-Host "========================================"
Write-Host ""

if ($DashboardEnabled) {
    Start-DashboardStack `
        -EnableInflux $EnableInflux `
        -EnableTimescale $EnableTimescale

    if ($EnableTimescale -and -not $NoCleanDashboard) {
        Clear-TimescaleQ1Results
    }
}

if ($Engine -eq "flink") {
    Clear-RunningFlinkJobs
}

if (-not $NoResetTopic) {
    Write-Host "Reset Kafka topic flights..."

    Reset-KafkaTopic -Topic "flights"

    if ($EnableInflux) {
        Write-Host "Reset Kafka topic q1_results..."
        Reset-KafkaTopic -Topic "q1_results"
    }

    Start-Sleep -Seconds 3

    Invoke-Checked {
        docker compose run --rm kafka-init
    }

    if ($EnableInflux) {
        Ensure-KafkaTopic -Topic "q1_results" -Partitions 4 -ReplicationFactor 1
    }
}
else {
    Write-Host "Kafka topic reset skipped."

    if ($EnableInflux) {
        Ensure-KafkaTopic -Topic "q1_results" -Partitions 4 -ReplicationFactor 1
    }
}

if (-not $NoPreprocess) {
    Write-Host ""
    Write-Host "Running preprocessing with base config..."

    Invoke-Checked {
        docker compose run --rm --build `
            -e CONFIG_PATH=/config/base.yml `
            preprocess
    }
}
else {
    Write-Host ""
    Write-Host "Preprocessing skipped."
}

if ($Engine -eq "flink") {
    Write-Host ""
    Write-Host "Submitting Flink $Query job ($Implementation)..."

    Invoke-Checked {
        docker compose run --rm --build `
            -e CONFIG_PATH=$SubmitCfgContainer `
            $RunSpec.Service
    }
}
else {
    Write-Host ""
    Write-Host "Starting Spark Structured $Query job..."

    Start-SparkJob `
        -ConfigPathContainer $SubmitCfgContainer `
        -Service $RunSpec.Service
}

# Perf monitor (Flink): sample throughput/latency via REST WHILE the producer
# replays and the job consumes. Spark writes its own perf row from the job.
$PerfProcess = $null
$PerfLog = $null
if ($Engine -eq "flink" -and -not $NoPerf) {
    Write-Host ""
    Write-Host "Starting perf monitor (report_perf.py) in background..."

    $PerfLog = Join-Path $env:TEMP "sabd_perf_$PID.log"
    $PerfArgs = @(
        ".\scripts\report_perf.py",
        "--engine", "flink",
        "--query", $Query,
        "--implementation", $Implementation,
        "--exp", $Label,
        "--parallelism", "0"
    )

    $PerfProcess = Start-Process -FilePath "python" -ArgumentList $PerfArgs `
        -NoNewWindow -PassThru `
        -RedirectStandardOutput $PerfLog `
        -RedirectStandardError "$PerfLog.err"
}

Write-Host ""
Write-Host "Running producer..."

Invoke-Checked {
    docker compose run --rm --build `
        -e CONFIG_PATH=$SubmitCfgContainer `
        producer
}

if ($null -ne $PerfProcess) {
    Write-Host ""
    Write-Host "Waiting for perf monitor to finish (job drain + idle)..."

    if (-not $PerfProcess.WaitForExit(180000)) {
        Write-Warning "Perf monitor still running after 180s; killing it."
        try { $PerfProcess.Kill() } catch { }
    }

    foreach ($LogPath in @($PerfLog, "$PerfLog.err")) {
        if ($LogPath -and (Test-Path $LogPath)) {
            Get-Content $LogPath | Where-Object { $_ } | ForEach-Object { Write-Host $_ }
            Remove-Item $LogPath -ErrorAction SilentlyContinue
        }
    }
}

Write-Host ""
if ($Engine -eq "flink") {
    Write-Host "Flink running jobs:"

    Invoke-Checked {
        docker exec flink-jobmanager flink list -r
    }
}
else {
    Write-Host "Waiting for Spark $Query job to finish..."
    Wait-SparkJob `
        -TimeoutSeconds $SparkTimeoutSeconds `
        -Container $RunSpec.Container
}

if (-not $NoMerge) {
    Write-Host ""
    Write-Host "Merging $Query results for $Engine/$Implementation (waiting for part files to stabilize)..."

    Invoke-Checked {
        python $RunSpec.MergeScript @MergeArgs --wait --timeout $MergeTimeoutSeconds
    }
}
else {
    Write-Host ""
    Write-Host "Merge skipped."
}

# numLateRecordsDropped e' esposto dal WindowOperator della Table/SQL API
# (Q1 table: WindowAggregate, Q2 table: WindowRank), che lo registra da se'.
# Il window operator di PyFlink DataStream (q1/q2 datastream) NON lo registra.
# Q3 (finestre DataStream per il DDSketch) lo ricrea a mano: side output dei
# late data -> Q3LateDropCounter incrementa un Counter 'numLateRecordsDropped'
# su un operatore nominato senza virgole (Q3LateDrops[...]), leggibile via REST.
$CollectLateDrops = (
    $Engine -eq "flink" -and (
        ($Implementation -eq "table" -and ($Query -eq "q1" -or $Query -eq "q2")) -or
        ($Query -eq "q3")
    )
)

if ($CollectLateDrops) {
    Write-Host ""
    Write-Host "Collecting Flink late-drop metrics (numLateRecordsDropped)..."

    python .\scripts\report_late_drops.py @MergeArgs

    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Late-drop metrics collection failed (exit code $LASTEXITCODE); continuing."
    }
}
else {
    Write-Host ""
    Write-Host "Late-drop metrics skipped for $Engine/$Implementation/$Query (numLateRecordsDropped exposed only by q1/q2 table and by q3)."
}

if ($Engine -eq "flink" -and -not $KeepFlinkJob) {
    Write-Host ""
    Write-Host "Cancelling Flink jobs after experiment..."
    Clear-RunningFlinkJobs
}
elseif ($Engine -eq "flink") {
    Write-Host ""
    Write-Host "Flink job left running because -KeepFlinkJob was specified."
}

Write-Host ""
Write-Host "Done: $Label [$Query/$Engine/$Implementation]"
