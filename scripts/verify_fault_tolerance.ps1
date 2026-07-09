<#
verify_fault_tolerance.ps1

Verifica sperimentale della tolleranza ai guasti degli operatori con stato di
Apache Flink (meccanismo di checkpointing). Scenario "guasto di nodo": a job in
esecuzione, uccide il container del TaskManager DOPO che e' stato completato
almeno un checkpoint; la restart strategy configurata (config/base.yml -> flink)
riavvia il job che RIPRISTINA lo stato dall'ultimo checkpoint e riprende dagli
offset Kafka salvati, senza perdita ne duplicazione dei risultati.

Va lanciato dalla root del progetto (dove sta docker-compose.yml).

FLUSSO
    1. avvia il cluster Flink (jobmanager + taskmanager);
    2. resetta il topic Kafka e (opzionale) esegue il preprocessing;
    3. sottomette il job Q1 al cluster (flink run -d);
    4. avvia il producer RALLENTATO in background (cosi' il guasto cade durante
       l'elaborazione e dopo il primo checkpoint);
    5. attende via REST che il job sia RUNNING e che i checkpoint completati
       siano >= -CheckpointsBeforeFault; registra il baseline (numRestarts=0);
    6. GUASTO: docker kill flink-taskmanager -> il job lascia lo stato RUNNING;
    7. riavvia il TaskManager (docker start) e ne attende la registrazione;
    8. attende che il job torni RUNNING e che 'latest.restored' del checkpoint
       sia valorizzato (prova del ripristino dello stato dall'ultimo checkpoint)
       e che numRestarts sia >= 1;
    9. attende la fine del producer, poi (opz.) fa il merge dell'output.

    Esito PASS se: (a) il job ha ripristinato uno stato da checkpoint dopo il
    guasto (latest.restored valorizzato) e (b) numRestarts e' aumentato.

USO
    # Verifica completa (default: q1, producer 2000x, guasto dopo 1 checkpoint)
    .\scripts\verify_fault_tolerance.ps1

    # Salta il preprocessing se i dati sono gia' pronti
    .\scripts\verify_fault_tolerance.ps1 -NoPreprocess

    # Baseline SENZA guasto (per confrontare l'output con quello del run con guasto)
    .\scripts\verify_fault_tolerance.ps1 -NoFault -NoPreprocess

    # Confronto tempi di sola esecuzione: run con guasto e baseline senza guasto
    .\scripts\verify_fault_tolerance.ps1 -NoPreprocess -NoMerge
    .\scripts\verify_fault_tolerance.ps1 -NoFault -NoPreprocess -NoMerge

OPZIONI
    -Query                   q1 (default). Riservato per estensioni future.
    -AccelerationFactor      Fattore di accelerazione del producer per la demo.
                             Piu' basso = run piu' lunga = guasto piu' facile da
                             collocare. Default 2000 (base.yml usa 57600).
    -CheckpointsBeforeFault  Checkpoint completati da attendere prima del guasto.
                             Default 1.
    -NoPreprocess            Non rilancia il preprocessing.
    -NoResetTopic            Non resetta il topic Kafka flights.
    -NoFault                 Non inietta il guasto (produce un output baseline).
    -NoMerge                 Non crea il CSV finale.
    -KeepJob                 Non cancella il job Flink a fine verifica.
    -FlinkUrl                REST del JobManager. Default http://localhost:8081.
    -TimingCsv               CSV dove appendere i tempi di sola esecuzione.
                             Default Results/fault_tolerance_timing.csv.
#>
param(
    [ValidateSet("q1")]
    [string]$Query = "q1",

    [int]$AccelerationFactor = 57600,
    [int]$CheckpointsBeforeFault = 1,

    [switch]$NoPreprocess,
    [switch]$NoResetTopic,
    [switch]$NoFault,
    [switch]$NoMerge,
    [switch]$KeepJob,

    [string]$FlinkUrl = "http://localhost:8081",

    [string]$TimingCsv = "Results/fault_tolerance_timing.csv"
)

$ErrorActionPreference = "Stop"
$FlinkUrl = $FlinkUrl.TrimEnd("/")

# ── Helper ──────────────────────────────────────────────────────────────────

function Invoke-Checked {
    param([Parameter(Mandatory = $true)][scriptblock]$Command)
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code $LASTEXITCODE"
    }
}

function Assert-ProjectRoot {
    if (-not (Test-Path ".\docker-compose.yml")) {
        throw "Esegui lo script dalla root del progetto (dove sta docker-compose.yml)."
    }
}

function Get-FlinkJson {
    param([Parameter(Mandatory = $true)][string]$Path)
    return Invoke-RestMethod -Uri "$FlinkUrl$Path" -TimeoutSec 10
}

function Get-RunningJob {
    # Ritorna l'oggetto job RUNNING che matcha la query, o $null.
    try {
        $overview = Get-FlinkJson -Path "/jobs/overview"
    }
    catch {
        return $null
    }
    $running = @($overview.jobs | Where-Object { $_.state -eq "RUNNING" })
    if ($running.Count -eq 0) {
        return $null
    }
    $match = @($running | Where-Object { $_.name -match $Query })
    if ($match.Count -gt 0) {
        return $match[0]
    }
    return $running[0]
}

function Get-JobState {
    param([Parameter(Mandatory = $true)][string]$Jid)
    try {
        return (Get-FlinkJson -Path "/jobs/$Jid").state
    }
    catch {
        return $null
    }
}

function Get-CheckpointStats {
    param([Parameter(Mandatory = $true)][string]$Jid)
    try {
        return Get-FlinkJson -Path "/jobs/$Jid/checkpoints"
    }
    catch {
        return $null
    }
}

function Get-NumRestarts {
    param([Parameter(Mandatory = $true)][string]$Jid)
    try {
        $m = Get-FlinkJson -Path "/jobs/$Jid/metrics?get=numRestarts"
        $val = ($m | Where-Object { $_.id -eq "numRestarts" }).value
        if ($null -ne $val) { return [int]$val }
    }
    catch { }
    return 0
}

function Get-RegisteredTaskManagers {
    try {
        return @((Get-FlinkJson -Path "/taskmanagers").taskmanagers).Count
    }
    catch {
        return 0
    }
}

function Cancel-FlinkJobBestEffort {
    param([Parameter(Mandatory = $true)][string]$Jid)

    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = @(docker exec flink-jobmanager flink cancel $Jid 2>&1)
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    if ($exitCode -ne 0) {
        Write-Warning "Cancel job $Jid fallito (exit $exitCode): $($output -join ' ')"
    }
}

function New-FaultRuntimeConfig {
    # Config runtime che estende base.yml: consumer group unico + producer
    # rallentato per la demo. Stesso schema di run_experiment.ps1.
    param([Parameter(Mandatory = $true)][int]$Acceleration)

    $runtimeDir = "config/runtime"
    New-Item -ItemType Directory -Force -Path $runtimeDir | Out-Null

    $runId = "$(Get-Date -Format 'yyyyMMddHHmmss')-$PID"
    $fileName = "fault.$runId.runtime.yml"
    $hostPath = Join-Path $runtimeDir $fileName

    $content = @"
extends: "../base.yml"

flink:
  consumer_group: "flink-fault-$runId"

producer:
  acceleration_factor: $Acceleration

dashboard:
  influx:
    enabled: false
  timescale:
    enabled: false
"@
    Set-Content -Path $hostPath -Value $content -Encoding UTF8

    return @{
        HostPath      = $hostPath
        ContainerPath = "/config/runtime/$fileName"
    }
}

# ── Preludio ────────────────────────────────────────────────────────────────

function Write-TimingCsvRow {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][pscustomobject]$Row
    )

    if ([string]::IsNullOrWhiteSpace($Path)) {
        return
    }

    $parent = Split-Path -Parent $Path
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }

    $Row | Export-Csv -Path $Path -NoTypeInformation -Append -Encoding UTF8
}

function Get-StopwatchElapsedMs {
    param([System.Diagnostics.Stopwatch]$Stopwatch)

    if ($null -eq $Stopwatch) {
        return $null
    }

    return [int64][math]::Round($Stopwatch.Elapsed.TotalMilliseconds)
}

Assert-ProjectRoot

Write-Host ""
Write-Host "========================================================"
Write-Host " Verifica tolleranza ai guasti (checkpointing di Flink)"
Write-Host " Query               : $Query"
Write-Host " Producer accel.      : ${AccelerationFactor}x"
Write-Host " Checkpoint pre-guasto: $CheckpointsBeforeFault"
Write-Host " Modalita'            : $(if ($NoFault) { 'BASELINE (nessun guasto)' } else { 'GUASTO (kill TaskManager)' })"
Write-Host "========================================================"
Write-Host ""

Write-Host "Avvio cluster Flink (jobmanager + taskmanager)..."
Invoke-Checked {
    docker compose up -d `
        kafka kafka2 schema-registry schema-init flink-jobmanager flink-taskmanager
}

# Attende la REST del JobManager.
Write-Host "Attendo la REST del JobManager su $FlinkUrl ..."
$deadline = (Get-Date).AddSeconds(120)
while ((Get-Date) -lt $deadline) {
    try {
        Get-FlinkJson -Path "/overview" | Out-Null
        break
    }
    catch {
        Start-Sleep -Seconds 2
    }
}

# Cancella eventuali job precedenti per partire pulito.
$existing = Get-RunningJob
if ($null -ne $existing) {
    Write-Host "Cancello il job precedente $($existing.jid)..."
    Cancel-FlinkJobBestEffort -Jid $existing.jid
    Start-Sleep -Seconds 5
}

$runtimeCfg = New-FaultRuntimeConfig -Acceleration $AccelerationFactor
$cfgContainer = $runtimeCfg.ContainerPath

if (-not $NoResetTopic) {
    Write-Host "Reset del topic Kafka flights..."
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic flights --if-exists 2>&1 | Out-Null
    Start-Sleep -Seconds 3
    Invoke-Checked { docker compose run --rm kafka-init }
}

if (-not $NoPreprocess) {
    Write-Host "Preprocessing..."
    Invoke-Checked {
        docker compose run --rm --build -e CONFIG_PATH=/config/base.yml preprocess
    }
}
else {
    Write-Host "Preprocessing saltato (-NoPreprocess)."
}

# ── Submit del job Q1 ───────────────────────────────────────────────────────

Write-Host ""
Write-Host "Sottometto il job Q1 al cluster..."
Invoke-Checked {
    docker compose run --rm --build -e CONFIG_PATH=$cfgContainer "flink-job-$Query"
}

# Attende che il job sia RUNNING.
Write-Host "Attendo che il job sia RUNNING..."
$job = $null
$deadline = (Get-Date).AddSeconds(180)
while ((Get-Date) -lt $deadline) {
    $job = Get-RunningJob
    if ($null -ne $job) { break }
    Start-Sleep -Seconds 3
}
if ($null -eq $job) {
    throw "Nessun job RUNNING entro il timeout: submit fallita? (docker logs flink-job-$Query)"
}
$jid = $job.jid
Write-Host "Job RUNNING: $jid ($($job.name))"

# ── Producer in background (rallentato) ─────────────────────────────────────

Write-Host ""
Write-Host "Avvio il producer in background (${AccelerationFactor}x)..."
$producerLog = Join-Path $env:TEMP "sabd_fault_producer_$PID.log"
$executionStartedAtUtc = (Get-Date).ToUniversalTime()
$executionStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
$producerStopwatch = [System.Diagnostics.Stopwatch]::StartNew()
$producerElapsedMs = $null
$producerTimedOut = $false
$producerExitCode = $null
$faultInjectedAtMs = $null
$faultDetectedAtMs = $null
$restoreDetectedAtMs = $null
$recoveryElapsedMs = $null
$restartsAfter = $null
$faultPass = $null
$producerProc = Start-Process -FilePath "docker" `
    -ArgumentList @("compose", "run", "--rm", "-e", "CONFIG_PATH=$cfgContainer", "producer") `
    -NoNewWindow -PassThru `
    -RedirectStandardOutput $producerLog `
    -RedirectStandardError "$producerLog.err"

# ── Attesa del primo checkpoint ─────────────────────────────────────────────

Write-Host ""
Write-Host "Attendo >= $CheckpointsBeforeFault checkpoint completati prima del guasto..."
$deadline = (Get-Date).AddSeconds(180)
$completedBefore = 0
while ((Get-Date) -lt $deadline) {
    $state = Get-JobState -Jid $jid
    if ($state -ne "RUNNING") {
        if ($producerProc.HasExited -and $state -in @("FINISHED", "CANCELED")) {
            throw "Il job e' terminato ($state) prima del guasto: producer troppo veloce. Riprova con -AccelerationFactor piu' basso."
        }
        Start-Sleep -Seconds 2
        continue
    }
    $ckp = Get-CheckpointStats -Jid $jid
    if ($null -ne $ckp) {
        $completedBefore = [int]$ckp.counts.completed
        if ($completedBefore -ge $CheckpointsBeforeFault) {
            $lastId = $ckp.latest.completed.id
            Write-Host "Checkpoint completati: $completedBefore (ultimo id=$lastId)."
            break
        }
    }
    Start-Sleep -Seconds 3
}
if ($completedBefore -lt $CheckpointsBeforeFault) {
    throw "Non sono stati completati abbastanza checkpoint entro il timeout (visti: $completedBefore)."
}

$restartsBefore = Get-NumRestarts -Jid $jid
$restartsAfter = $restartsBefore
Write-Host "Baseline: numRestarts=$restartsBefore, checkpoint completati=$completedBefore."

if ($NoFault) {
    Write-Host ""
    Write-Host "Modalita' BASELINE: nessun guasto iniettato. Attendo la fine del producer..."
}
else {
    # ── GUASTO: kill del TaskManager ────────────────────────────────────────
    Write-Host ""
    Write-Host ">>> GUASTO: docker kill flink-taskmanager  ($(Get-Date -Format 'HH:mm:ss'))"
    $faultInjectedAtMs = Get-StopwatchElapsedMs -Stopwatch $executionStopwatch
    Invoke-Checked { docker kill flink-taskmanager }

    # Osserva il job lasciare lo stato RUNNING.
    Write-Host "Attendo che il job rilevi il guasto (uscita da RUNNING)..."
    $deadline = (Get-Date).AddSeconds(60)
    $failedState = $null
    while ((Get-Date) -lt $deadline) {
        $s = Get-JobState -Jid $jid
        if ($s -and $s -ne "RUNNING") {
            $failedState = $s
            $faultDetectedAtMs = Get-StopwatchElapsedMs -Stopwatch $executionStopwatch
            Write-Host "Job in stato '$s' dopo il guasto."
            break
        }
        Start-Sleep -Seconds 2
    }
    if ($null -eq $failedState) {
        Write-Warning "Il job e' ancora RUNNING: il guasto potrebbe non aver colpito operatori attivi."
    }

    # ── Riavvio del TaskManager ─────────────────────────────────────────────
    Write-Host ">>> Riavvio del TaskManager: docker start flink-taskmanager"
    Invoke-Checked { docker start flink-taskmanager }

    Write-Host "Attendo la ri-registrazione del TaskManager..."
    $deadline = (Get-Date).AddSeconds(120)
    while ((Get-Date) -lt $deadline) {
        if ((Get-RegisteredTaskManagers) -ge 1) {
            Write-Host "TaskManager registrato."
            break
        }
        Start-Sleep -Seconds 3
    }

    # ── Verifica del ripristino dallo checkpoint ────────────────────────────
    Write-Host "Attendo il ripristino del job dall'ultimo checkpoint..."
    $deadline = (Get-Date).AddSeconds(180)
    $restored = $null
    $restartsAfter = $restartsBefore
    while ((Get-Date) -lt $deadline) {
        $s = Get-JobState -Jid $jid
        $ckp = Get-CheckpointStats -Jid $jid
        if ($null -ne $ckp -and $null -ne $ckp.latest.restored) {
            $restored = $ckp.latest.restored
        }
        $restartsAfter = Get-NumRestarts -Jid $jid
        if ($s -eq "RUNNING" -and $null -ne $restored -and $restartsAfter -gt $restartsBefore) {
            $restoreDetectedAtMs = Get-StopwatchElapsedMs -Stopwatch $executionStopwatch
            break
        }
        Start-Sleep -Seconds 3
    }

    if ($null -ne $faultInjectedAtMs -and $null -ne $restoreDetectedAtMs) {
        $recoveryElapsedMs = $restoreDetectedAtMs - $faultInjectedAtMs
    }

    Write-Host ""
    Write-Host "----------------------- ESITO --------------------------"
    $pass = $true
    if ($null -ne $restored) {
        Write-Host "[OK] Stato ripristinato dal checkpoint id=$($restored.id)"
        Write-Host "     restore_timestamp=$($restored.restore_timestamp)  external_path=$($restored.external_path)"
    }
    else {
        Write-Host "[FAIL] Nessun ripristino da checkpoint rilevato (latest.restored nullo)."
        $pass = $false
    }
    if ($restartsAfter -gt $restartsBefore) {
        Write-Host "[OK] numRestarts: $restartsBefore -> $restartsAfter (il job e' stato riavviato)."
    }
    else {
        Write-Host "[FAIL] numRestarts non aumentato ($restartsAfter): nessun riavvio rilevato."
        $pass = $false
    }
    Write-Host "--------------------------------------------------------"
    Write-Host $(if ($pass) { "VERIFICA SUPERATA: recupero dello stato dopo il guasto OK." } `
                 else { "VERIFICA FALLITA: vedi i marker [FAIL] sopra." })
    Write-Host "--------------------------------------------------------"
    $faultPass = $pass
}

# ── Attesa fine producer ────────────────────────────────────────────────────

Write-Host ""
Write-Host "Attendo la fine del producer..."
if (-not $producerProc.WaitForExit(600000)) {
    Write-Warning "Producer ancora in esecuzione dopo 600s; lo termino."
    $producerTimedOut = $true
    try {
        $producerProc.Kill()
        $producerProc.WaitForExit(30000) | Out-Null
        $producerProc.Refresh()
        $producerExitCode = $producerProc.ExitCode
    }
    catch { }
}
else {
    $producerProc.Refresh()
    $producerExitCode = $producerProc.ExitCode
}
$producerStopwatch.Stop()
$producerElapsedMs = Get-StopwatchElapsedMs -Stopwatch $producerStopwatch
foreach ($log in @($producerLog, "$producerLog.err")) {
    if ($log -and (Test-Path $log)) { Remove-Item $log -ErrorAction SilentlyContinue }
}

# Concede al job il tempo di consolidare le finestre finali (EOS -> watermark).
Write-Host "Attendo il consolidamento delle finestre finali (15s)..."
Start-Sleep -Seconds 15
$executionStopwatch.Stop()
$executionEndedAtUtc = (Get-Date).ToUniversalTime()
$executionElapsedMs = Get-StopwatchElapsedMs -Stopwatch $executionStopwatch

Write-Host ""
Write-Host ("Tempo sola esecuzione (producer -> consolidamento finale, merge escluso): {0:N3}s" -f ($executionElapsedMs / 1000.0))
Write-Host ("Tempo producer: {0:N3}s" -f ($producerElapsedMs / 1000.0))
if ($null -ne $recoveryElapsedMs) {
    Write-Host ("Tempo recovery dal guasto: {0:N3}s" -f ($recoveryElapsedMs / 1000.0))
}

$timingMode = if ($NoFault) { "without_fault" } else { "with_fault" }
$timingRow = [pscustomobject][ordered]@{
    timestamp_utc = $executionEndedAtUtc.ToString("o")
    mode = $timingMode
    query = $Query
    acceleration_factor = $AccelerationFactor
    checkpoints_before_fault = $CheckpointsBeforeFault
    job_id = $jid
    execution_started_utc = $executionStartedAtUtc.ToString("o")
    execution_ended_utc = $executionEndedAtUtc.ToString("o")
    execution_elapsed_ms = $executionElapsedMs
    producer_elapsed_ms = $producerElapsedMs
    completed_checkpoints_before_fault = $completedBefore
    restarts_before = $restartsBefore
    restarts_after = $restartsAfter
    fault_injected_at_ms = $faultInjectedAtMs
    fault_detected_at_ms = $faultDetectedAtMs
    restore_detected_at_ms = $restoreDetectedAtMs
    recovery_elapsed_ms = $recoveryElapsedMs
    producer_timed_out = $producerTimedOut
    producer_exit_code = $producerExitCode
    verification_pass = $faultPass
    notes = "execution=producer_start_to_final_window_consolidation; excludes docker_setup, topic_reset, preprocessing, job_submit, merge, cleanup"
}
Write-TimingCsvRow -Path $TimingCsv -Row $timingRow
Write-Host "Tempi appendati in $TimingCsv"

if (-not $NoMerge) {
    Write-Host ""
    Write-Host "Merge dell'output Q1..."
    python .\scripts\merge_q1.py --wait --timeout 180
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Merge fallito (exit $LASTEXITCODE)."
    }
}

if (-not $KeepJob) {
    Write-Host ""
    Write-Host "Cancello il job Flink..."
    $j = Get-RunningJob
    if ($null -ne $j) {
        Cancel-FlinkJobBestEffort -Jid $j.jid
    }
}
else {
    Write-Host "Job lasciato in esecuzione (-KeepJob)."
}

Write-Host ""
Write-Host "Fatto."
