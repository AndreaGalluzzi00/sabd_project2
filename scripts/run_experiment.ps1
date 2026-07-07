<#
run_experiment.ps1

Runner unico per eseguire una query (q1/q2/q3) su Flink o Spark, con una
config di esperimento opzionale. Va lanciato dalla root del progetto, cioe'
dalla cartella che contiene docker-compose.yml.

USO RAPIDO
    # Default: q1 + flink + table + config/base.yml
    .\scripts\run_experiment.ps1

    # Stesso scenario sperimentale, motori diversi
    .\scripts\run_experiment.ps1 -e 01_baseline -Query q1 -Engine flink -Implementation table
    .\scripts\run_experiment.ps1 -e 01_baseline -Query q1 -Engine spark

    # Q2 / Q3
    .\scripts\run_experiment.ps1 -Query q2 -Engine flink
    .\scripts\run_experiment.ps1 -Query q2 -Engine spark
    .\scripts\run_experiment.ps1 -Query q3 -Engine flink
    .\scripts\run_experiment.ps1 -Query q3 -Engine spark

COMBINAZIONI SUPPORTATE
    Flink:
        q1 table
        q2 table
        q3 table

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
    I part-file vengono puliti a inizio run, salvo -NoCleanResults, e restano
    separati per esperimento/engine/implementazione/query.

    Esempi part-file per 01_baseline:
        Results/experiments/01_baseline/flink/table/q1/...
        Results/experiments/01_baseline/spark/structured/q1/...

    I CSV finali dei merge vengono invece salvati direttamente in Results.

    Esempi CSV finali:
        Results/q1_flink_table_01_baseline.csv
        Results/q1_spark_structured_01_baseline.csv
        Results/q2_1h_flink_table_01_baseline.csv

OPZIONI UTILI
    -NoPreprocess          Salta il preprocessing se e' gia' stato fatto.
    -NoResetTopic          Non resetta il topic Kafka flights.
    -NoCleanResults        Non cancella i part-file precedenti.
    -NoCleanDashboard      Non svuota InfluxDB/TimescaleDB prima della run.
    -NoMerge               Non crea il CSV finale.
    -MergeTimeoutSeconds   Timeout per attendere part-file stabili. Default: 180.
    -SparkTimeoutSeconds   Timeout per attendere la fine di Spark. Default: 1200.
    -KeepFlinkJob          Non cancella il job Flink a fine run.

DASHBOARD
    Le dashboard sono supportate per i job Flink table di Q1, Q2 e Q3:
        -Query q1|q2|q3 -Engine flink -Implementation table

    Esempi:
        .\scripts\run_experiment.ps1 -e 05_wm_safe -FullFlow
        .\scripts\run_experiment.ps1 -e 05_wm_safe -Query q2 -FullFlow
        .\scripts\run_experiment.ps1 -e 05_wm_safe -Query q3 -FullFlow
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

    [ValidateSet("", "table", "structured")]
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

function Start-ExperimentInfrastructure {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("flink", "spark")]
        [string]$Engine
    )

    Write-Host ""

    if ($Engine -eq "flink") {
        Write-Host "Starting Kafka, Schema Registry and Flink infrastructure..."

        Invoke-Checked {
            docker compose up -d `
                kafka kafka2 schema-registry schema-init flink-jobmanager flink-taskmanager
        }

        return
    }

    Write-Host "Starting Kafka and Schema Registry infrastructure..."

    Invoke-Checked {
        docker compose up -d `
            kafka kafka2 schema-registry schema-init
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

function Wait-InfluxDb {
    for ($Attempt = 1; $Attempt -le 30; $Attempt++) {
        docker exec sabd2-influxdb influx ping --host http://localhost:8086 | Out-Null

        if ($LASTEXITCODE -eq 0) {
            return
        }

        Start-Sleep -Seconds 2
    }

    throw "InfluxDB non pronto dopo 60 secondi."
}

function Get-DashboardKafkaTopics {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("q1", "q2", "q3")]
        [string]$Query
    )

    switch ($Query) {
        "q1" {
            return @(
                [pscustomobject]@{ Name = "q1_results"; Partitions = 4 }
            )
        }
        "q2" {
            return @(
                [pscustomobject]@{ Name = "q2_results_1h"; Partitions = 1 },
                [pscustomobject]@{ Name = "q2_results_6h"; Partitions = 1 },
                [pscustomobject]@{ Name = "q2_results_global"; Partitions = 1 }
            )
        }
        "q3" {
            return @(
                [pscustomobject]@{ Name = "q3_results_1d"; Partitions = 1 },
                [pscustomobject]@{ Name = "q3_results_7d"; Partitions = 1 },
                [pscustomobject]@{ Name = "q3_results_global"; Partitions = 1 }
            )
        }
    }
}

function Get-DashboardTelegrafConsumerGroups {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("q1", "q2", "q3")]
        [string]$Query
    )

    switch ($Query) {
        "q1" { return @("telegraf-q1") }
        "q2" { return @("telegraf-q2-1h", "telegraf-q2-6h", "telegraf-q2-global") }
        "q3" { return @("telegraf-q3-1d", "telegraf-q3-7d", "telegraf-q3-global") }
    }
}

function Get-DashboardInfluxMeasurements {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("q1", "q2", "q3")]
        [string]$Query
    )

    switch ($Query) {
        "q1" { return @("q1") }
        "q2" { return @("q2_1h", "q2_6h", "q2_global") }
        "q3" { return @("q3_1d", "q3_7d", "q3_global") }
    }
}

function Get-DashboardTimescaleTables {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("q1", "q2", "q3")]
        [string]$Query
    )

    switch ($Query) {
        "q1" { return @("q1_results") }
        "q2" { return @("q2_results_1h", "q2_results_6h", "q2_results_global") }
        "q3" { return @("q3_results_1d", "q3_results_7d", "q3_results_global") }
    }
}

function Get-DashboardTimescaleInitScript {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("q1", "q2", "q3")]
        [string]$Query
    )

    switch ($Query) {
        "q1" { return "/docker-entrypoint-initdb.d/01_q1.sql" }
        "q2" { return "/docker-entrypoint-initdb.d/02_q2.sql" }
        "q3" { return "/docker-entrypoint-initdb.d/03_q3.sql" }
    }
}

function Ensure-TimescaleDashboardSchema {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("q1", "q2", "q3")]
        [string]$Query
    )

    $InitScript = Get-DashboardTimescaleInitScript -Query $Query

    Write-Host "Ensuring TimescaleDB schema for $Query..."

    Invoke-Checked {
        docker exec sabd2-timescaledb psql `
            -v ON_ERROR_STOP=1 `
            -U sabd `
            -d sabd `
            -f $InitScript
    }
}

function Clear-InfluxDashboardResults {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("q1", "q2", "q3")]
        [string]$Query
    )

    $Measurements = @(Get-DashboardInfluxMeasurements -Query $Query)

    Write-Host ""
    Write-Host "Cleaning InfluxDB dashboard measurements for ${Query}: $($Measurements -join ', ')"

    Wait-InfluxDb

    foreach ($Measurement in $Measurements) {
        $Predicate = "_measurement=`"$Measurement`""

        Invoke-Checked {
            docker exec sabd2-influxdb influx delete `
                --host http://localhost:8086 `
                --org sabd `
                --bucket flights `
                --token sabd-dev-token-please-change `
                --start 1970-01-01T00:00:00Z `
                --stop 2100-01-01T00:00:00Z `
                --predicate $Predicate
        }
    }
}

function Clear-TimescaleDashboardResults {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("q1", "q2", "q3")]
        [string]$Query
    )

    $Tables = @(Get-DashboardTimescaleTables -Query $Query)
    $Sql = "TRUNCATE TABLE " + ($Tables -join ", ") + ";"

    Write-Host ""
    Write-Host "Cleaning TimescaleDB dashboard tables for ${Query}: $($Tables -join ', ')"

    Wait-TimescaleDb
    Ensure-TimescaleDashboardSchema -Query $Query

    Invoke-Checked {
        docker exec sabd2-timescaledb psql `
            -v ON_ERROR_STOP=1 `
            -U sabd `
            -d sabd `
            -c $Sql
    }
}

function Stop-TelegrafDashboardBridge {
    Write-Host ""
    Write-Host "Stopping Telegraf dashboard bridge before Kafka result-topic reset..."

    Invoke-Checked {
        docker compose --profile dashboard-influx stop telegraf
    }
}

function Start-TelegrafDashboardBridge {
    Write-Host ""
    Write-Host "Starting Telegraf dashboard bridge with fresh Kafka offsets..."

    Invoke-Checked {
        docker compose --profile dashboard-influx up -d --no-deps --force-recreate telegraf
    }
}

function Clear-DashboardKafkaConsumerGroups {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("q1", "q2", "q3")]
        [string]$Query
    )

    foreach ($Group in @(Get-DashboardTelegrafConsumerGroups -Query $Query)) {
        Write-Host "Reset Telegraf Kafka consumer group $Group..."

        $PreviousErrorActionPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            $Output = @(docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh `
                --bootstrap-server kafka:9092 `
                --delete `
                --group $Group 2>&1)
            $ExitCode = $LASTEXITCODE
        }
        finally {
            $ErrorActionPreference = $PreviousErrorActionPreference
        }

        if ($ExitCode -eq 0) {
            continue
        }

        $Text = $Output -join " "
        if ($Text -match "does not exist|not found|Non-existing group|Group id .* not found") {
            Write-Host "Consumer group $Group already absent."
            continue
        }

        Write-Warning "Unable to delete consumer group $Group; continuing. Output: $Text"
    }
}

function Reset-DashboardKafkaTopics {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("q1", "q2", "q3")]
        [string]$Query
    )

    foreach ($TopicSpec in @(Get-DashboardKafkaTopics -Query $Query)) {
        Write-Host "Reset Kafka topic $($TopicSpec.Name)..."
        Reset-KafkaTopic -Topic $TopicSpec.Name
    }
}

function Ensure-DashboardKafkaTopics {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("q1", "q2", "q3")]
        [string]$Query
    )

    foreach ($TopicSpec in @(Get-DashboardKafkaTopics -Query $Query)) {
        Ensure-KafkaTopic `
            -Topic $TopicSpec.Name `
            -Partitions $TopicSpec.Partitions `
            -ReplicationFactor 1
    }
}

function Resolve-Implementation {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("flink", "spark")]
        [string]$Engine,

        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
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

    if ($Implementation -ne "table") {
        throw "Flink supporta solo -Implementation table."
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
            "q3_results_host_path_1d"
            "q3_results_host_path_7d"
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
        $Engine -ne "flink" `
        -or $Implementation -ne "table"
    )
) {
    throw "Le dashboard sono supportate da questo runner solo per -Query q1|q2|q3 -Engine flink -Implementation table."
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

    if ($EnableInflux -and -not $NoResetTopic) {
        Stop-TelegrafDashboardBridge
    }

    if ($EnableInflux -and -not $NoCleanDashboard) {
        Clear-InfluxDashboardResults -Query $Query
    }

    if ($EnableTimescale -and -not $NoCleanDashboard) {
        Clear-TimescaleDashboardResults -Query $Query
    }
}
else {
    Start-ExperimentInfrastructure -Engine $Engine
}

if ($Engine -eq "flink") {
    Clear-RunningFlinkJobs
}

if (-not $NoResetTopic) {
    Write-Host "Reset Kafka topic flights..."

    Reset-KafkaTopic -Topic "flights"

    if ($EnableInflux) {
        Clear-DashboardKafkaConsumerGroups -Query $Query
        Reset-DashboardKafkaTopics -Query $Query
    }

    Start-Sleep -Seconds 3

    Invoke-Checked {
        docker compose run --rm kafka-init
    }

    if ($EnableInflux) {
        Ensure-DashboardKafkaTopics -Query $Query
        Start-TelegrafDashboardBridge
    }
}
else {
    Write-Host "Kafka topic reset skipped."

    if ($EnableInflux) {
        Ensure-DashboardKafkaTopics -Query $Query
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

    if (-not $PerfProcess.WaitForExit(600000)) {
        Write-Warning "Perf monitor still running after 600s; killing it."
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

# Late-record drops (completezza sotto out-of-orderness):
#  - Q1/Q2 table: 'numLateRecordsDropped' e' registrata dal WindowOperator
#    della Table/SQL API.
#  - Q3: le finestre DDSketch sono DataStream, quindi il job registra un
#    counter equivalente sul side output dei late data (Q3LateDrops[...]).
# In entrambi i casi si legge live via REST prima di cancellare il job.
$CollectLateDropsMetric = (
    $Engine -eq "flink" -and $Implementation -eq "table"
)

if ($CollectLateDropsMetric) {
    Write-Host ""
    Write-Host "Collecting Flink late-drop metrics (numLateRecordsDropped)..."

    python .\scripts\report_late_drops.py @MergeArgs

    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Late-drop metrics collection failed (exit code $LASTEXITCODE); continuing."
    }
}
else {
    Write-Host ""
    Write-Host "Late-drop metrics skipped for $Engine/$Implementation/$Query."
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
