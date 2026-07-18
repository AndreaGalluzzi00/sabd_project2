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
    9. attende la fine del producer e almeno un checkpoint successivo, cosi'
       anche i risultati finali del sink filesystem sono consolidati;
   10. salva l'output in una directory isolata e, quando esiste la run gemella
       con lo stesso VerificationId, confronta part-file grezzi, duplicati,
       chiavi, valori e hash canonico.

    La prova end-to-end e' PASS se: (a) il job ha ripristinato lo stato da
    checkpoint; (b) numRestarts e' aumentato; (c) gli output con e senza guasto
    sono identici; (d) nessuno dei due output contiene chiavi duplicate.

USO
    # Verifica completa (default: q1, producer 57600x, guasto dopo 1 checkpoint)
    .\scripts\verify_fault_tolerance.ps1

    # Salta il preprocessing se i dati sono gia' pronti
    .\scripts\verify_fault_tolerance.ps1 -NoPreprocess

    # Baseline SENZA guasto (per confrontare l'output con quello del run con guasto)
    .\scripts\verify_fault_tolerance.ps1 -NoFault -NoPreprocess

    # Confronto tempi di sola esecuzione: run con guasto e baseline senza guasto
    .\scripts\verify_fault_tolerance.ps1 -NoPreprocess -NoMerge
    .\scripts\verify_fault_tolerance.ps1 -NoFault -NoPreprocess -NoMerge

    # Ripete lo stesso scenario piu' volte, appendendo una riga timing per run
    .\scripts\verify_fault_tolerance.ps1 -Runs 5 -NoPreprocess -NoMerge
    .\scripts\verify_fault_tolerance.ps1 -Runs 5 -NoFault -NoPreprocess -NoMerge

    # Verifica end-to-end di exactly-once (eseguire entrambe con lo stesso id).
    # Ogni run usa part-file isolati; quando esistono entrambi gli scenari lo
    # script confronta anche duplicati, chiavi, valori e hash canonico.
    .\scripts\verify_fault_tolerance.ps1 -NoFault -NoPreprocess -VerificationId q1_exactly_once
    .\scripts\verify_fault_tolerance.ps1 -NoPreprocess -VerificationId q1_exactly_once

OPZIONI
    -Query                   q1 (default). Riservato per estensioni future.
    -AccelerationFactor      Fattore di accelerazione del producer per la demo.
                             Piu' basso = run piu' lunga = guasto piu' facile da
                             collocare. Default 57600.
    -CheckpointsBeforeFault  Checkpoint completati da attendere prima del guasto.
                             Default 1.
    -Runs                    Numero di ripetizioni dello scenario. Default 1.
    -NoPreprocess            Non rilancia il preprocessing.
    -NoResetTopic            Non resetta il topic Kafka flights.
    -NoFault                 Non inietta il guasto (produce un output baseline).
    -NoMerge                 Non crea il CSV finale.
    -KeepJob                 Non cancella il job Flink a fine verifica.
    -FlinkUrl                REST del JobManager. Default http://localhost:8081.
    -TimingCsv               CSV dove appendere i tempi di sola esecuzione.
                             Default Results/fault_tolerance_timing.csv.
    -VerificationId          Identificatore condiviso dalla baseline e dalla run
                             con guasto. Default q1_checkpoint.
#>
param(
    [ValidateSet("q1")]
    [string]$Query = "q1",

    [int]$AccelerationFactor = 57600,
    [int]$CheckpointsBeforeFault = 1,
    [ValidateRange(1, 1000)]
    [int]$Runs = 1,

    [switch]$NoPreprocess,
    [switch]$NoResetTopic,
    [switch]$NoFault,
    [switch]$NoMerge,
    [switch]$KeepJob,

    [string]$FlinkUrl = "http://localhost:8081",

    [string]$TimingCsv = "Results/fault_tolerance_timing.csv",

    [string]$VerificationId = "q1_checkpoint"
)

$ErrorActionPreference = "Stop"
$FlinkUrl = $FlinkUrl.TrimEnd("/")


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

function Test-FinalizedPartFiles {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return $false
    }

    $files = @(Get-ChildItem -LiteralPath $Path -File | Where-Object {
        $_.Name -like "part-*" -and $_.Name -notlike "*.inprogress*"
    })
    return $files.Count -gt 0
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
    # rallentato per la demo. I path Q1 sono isolati per modalita'/run, cosi'
    # una verifica exactly-once non puo' mescolare output di esecuzioni diverse.
    param(
        [Parameter(Mandatory = $true)][int]$Acceleration,
        [Parameter(Mandatory = $true)][string]$Mode,
        [Parameter(Mandatory = $true)][int]$RunNumber,
        [Parameter(Mandatory = $true)][string]$Verification
    )

    $runtimeDir = "config/runtime"
    New-Item -ItemType Directory -Force -Path $runtimeDir | Out-Null

    $runId = "$(Get-Date -Format 'yyyyMMddHHmmssfff')-$PID"
    $fileName = "fault.$runId.runtime.yml"
    $hostPath = Join-Path $runtimeDir $fileName

    $runLabel = "run-{0:D2}" -f $RunNumber
    $verificationRoot = Join-Path "Results\fault_tolerance_verification" $Verification
    $hostRunRoot = Join-Path (Join-Path $verificationRoot $Mode) $runLabel
    $hostPartPath = Join-Path $hostRunRoot "part_files"
    $hostMergedPath = Join-Path $hostRunRoot "q1_base.csv"
    $comparisonRoot = Join-Path $verificationRoot "comparisons"
    $comparisonJson = Join-Path $comparisonRoot "$runLabel.json"
    $comparisonCsv = Join-Path $comparisonRoot "$runLabel.csv"

    # Rimuove soltanto l'output della stessa modalita'/run. L'altra meta' della
    # coppia resta disponibile per il confronto automatico.
    if (Test-Path -LiteralPath $hostRunRoot) {
        Remove-Item -LiteralPath $hostRunRoot -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path $hostPartPath | Out-Null
    New-Item -ItemType Directory -Force -Path $comparisonRoot | Out-Null
    foreach ($reportPath in @($comparisonJson, $comparisonCsv)) {
        if (Test-Path -LiteralPath $reportPath) {
            Remove-Item -LiteralPath $reportPath -Force
        }
    }

    $containerRunRoot = "/opt/flink/results/fault_tolerance_verification/$Verification/$Mode/$runLabel"
    $containerPartPath = "$containerRunRoot/part_files"
    $yamlHostPartPath = $hostPartPath.Replace("\", "/")
    $yamlHostMergedPath = $hostMergedPath.Replace("\", "/")

    $content = @"
extends: "../base.yml"

flink:
  consumer_group: "flink-fault-$runId"

producer:
  acceleration_factor: $Acceleration

paths:
  q1_results_path: "$containerPartPath"
  q1_results_host_path: "$yamlHostPartPath"
  q1_merged_output_host_path: "$yamlHostMergedPath"

dashboard:
  influx:
    enabled: false
  timescale:
    enabled: false
"@
    Set-Content -Path $hostPath -Value $content -Encoding UTF8

    return @{
        HostPath             = $hostPath
        ContainerPath        = "/config/runtime/$fileName"
        RunId                = $runId
        RunLabel             = $runLabel
        RunOutputHostPath    = $hostRunRoot
        PartHostPath         = $hostPartPath
        MergedHostPath       = $hostMergedPath
        ComparisonReportJson = $comparisonJson
        ComparisonReportCsv  = $comparisonCsv
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

if ($VerificationId -notmatch '^[A-Za-z0-9._-]+$') {
    throw "-VerificationId puo' contenere soltanto lettere, numeri, punto, trattino e underscore."
}

if ($Runs -gt 1 -and $KeepJob) {
    throw "-KeepJob non e' compatibile con -Runs > 1: ogni run deve ripartire da uno stato pulito."
}

if ($Runs -gt 1 -and $NoResetTopic) {
    Write-Warning "-NoResetTopic con -Runs > 1 puo' rendere le run non confrontabili perche' il topic Kafka non viene svuotato tra una ripetizione e l'altra."
}

for ($runIndex = 1; $runIndex -le $Runs; $runIndex++) {

$timingMode = if ($NoFault) { "without_fault" } else { "with_fault" }

Write-Host ""
Write-Host "========================================================"
Write-Host " Verifica tolleranza ai guasti (checkpointing di Flink)"
Write-Host " Run                 : $runIndex / $Runs"
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

$runtimeCfg = New-FaultRuntimeConfig `
    -Acceleration $AccelerationFactor `
    -Mode $timingMode `
    -RunNumber $runIndex `
    -Verification $VerificationId
$cfgContainer = $runtimeCfg.ContainerPath
Write-Host "Output isolato       : $($runtimeCfg.RunOutputHostPath)"

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
$runFailureMessages = @()
$producerContainer = "producer-fault-$($runtimeCfg.RunId)"
$producerProc = Start-Process -FilePath "docker" `
    -ArgumentList @("compose", "run", "--name", $producerContainer, "-e", "CONFIG_PATH=$cfgContainer", "producer") `
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
    if (-not $faultPass) {
        $runFailureMessages += "Ripristino da checkpoint non verificato."
    }
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
    }
    catch { }
}

# Start-Process/Process.ExitCode puo' restituire $null in Windows PowerShell
# dopo una WaitForExit temporizzata. Il container viene quindi mantenuto fino
# all'inspect: la sua State.ExitCode e' l'autorita' sul risultato del producer.
$producerStateText = @(docker inspect --format "{{.State.Status}} {{.State.ExitCode}}" $producerContainer 2>$null)
$producerInspectExitCode = $LASTEXITCODE
if ($producerInspectExitCode -eq 0 -and $producerStateText.Count -gt 0) {
    $producerStateParts = $producerStateText[-1].Trim() -split '\s+'
    if ($producerStateParts.Count -ge 2 -and $producerStateParts[0] -eq "exited") {
        $producerExitCode = [int]$producerStateParts[1]
    }
}
$producerSucceeded = (-not $producerTimedOut) -and ($producerExitCode -eq 0)
if (-not $producerSucceeded) {
    $runFailureMessages += "Producer non completato correttamente (timeout=$producerTimedOut, exit_code=$producerExitCode)."
}
$producerStopwatch.Stop()
$producerElapsedMs = Get-StopwatchElapsedMs -Stopwatch $producerStopwatch
foreach ($log in @($producerLog, "$producerLog.err")) {
    if ($log -and (Test-Path $log)) {
        if (-not $producerSucceeded) {
            Get-Content -LiteralPath $log -Tail 40 | Where-Object { $_ } | ForEach-Object { Write-Host $_ }
        }
        Remove-Item $log -ErrorAction SilentlyContinue
    }
}

# Il nome contiene il run id univoco; viene rimosso solo il container producer
# appena ispezionato, non altri servizi del progetto.
docker rm -f $producerContainer 2>&1 | Out-Null

if ($producerSucceeded) {
    # Dopo l'EOS lascia terminare le finestre finali e il rollover temporale del
    # sink. Solo dopo attende un checkpoint ulteriore: il sink filesystem di Flink
    # rende definitivi i file in corrispondenza dei checkpoint, quindi l'ordine
    # rollover -> checkpoint evita di confrontare soltanto un prefisso dell'output.
    Write-Host "Attendo finestre finali e rollover del sink (15s)..."
    Start-Sleep -Seconds 15
    $checkpointStatsAfterRollover = Get-CheckpointStats -Jid $jid
    $completedAfterRollover = if ($null -ne $checkpointStatsAfterRollover) {
        [int]$checkpointStatsAfterRollover.counts.completed
    }
    else {
        0
    }
    $targetCompletedAfterProducer = $completedAfterRollover + 1
    Write-Host "Attendo un checkpoint successivo all'EOS (target completati: $targetCompletedAfterProducer)..."
    $deadline = (Get-Date).AddSeconds(180)
    $completedAfterProducer = $completedAfterRollover
    while ((Get-Date) -lt $deadline) {
        $checkpointStats = Get-CheckpointStats -Jid $jid
        if ($null -ne $checkpointStats) {
            $completedAfterProducer = [int]$checkpointStats.counts.completed
            if ($completedAfterProducer -ge $targetCompletedAfterProducer) {
                break
            }
        }
        Start-Sleep -Seconds 2
    }
    if ($completedAfterProducer -lt $targetCompletedAfterProducer) {
        $runFailureMessages += "Nessun checkpoint completato dopo l'EOS entro 180s: output finale non verificabile."
    }
    else {
        Write-Host "Checkpoint post-EOS completato (totale: $completedAfterProducer)."
    }
}
else {
    Write-Host "Consolidamento output saltato perche' il producer non e' terminato correttamente."
}
$executionStopwatch.Stop()
$executionEndedAtUtc = (Get-Date).ToUniversalTime()
$executionElapsedMs = Get-StopwatchElapsedMs -Stopwatch $executionStopwatch

Write-Host ""
Write-Host ("Tempo sola esecuzione (producer -> consolidamento finale, merge escluso): {0:N3}s" -f ($executionElapsedMs / 1000.0))
Write-Host ("Tempo producer: {0:N3}s" -f ($producerElapsedMs / 1000.0))
if ($null -ne $recoveryElapsedMs) {
    Write-Host ("Tempo recovery dal guasto: {0:N3}s" -f ($recoveryElapsedMs / 1000.0))
}

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
    notes = "run=$runIndex/$Runs; verification_id=$VerificationId; output=$($runtimeCfg.RunOutputHostPath); execution=producer_start_to_post_eos_checkpoint_and_final_window_consolidation; excludes docker_setup, topic_reset, preprocessing, job_submit, merge, comparison, cleanup"
}
Write-TimingCsvRow -Path $TimingCsv -Row $timingRow
Write-Host "Tempi appendati in $TimingCsv"

if ((-not $NoMerge) -and $producerSucceeded -and ($runFailureMessages.Count -eq 0)) {
    Write-Host ""
    Write-Host "Merge dell'output Q1..."
    $previousConfigPath = $env:CONFIG_PATH
    try {
        $env:CONFIG_PATH = (Resolve-Path -LiteralPath $runtimeCfg.HostPath).Path
        Invoke-Checked { python .\scripts\merge_q1.py --wait --timeout 180 }
    }
    finally {
        if ($null -eq $previousConfigPath) {
            Remove-Item Env:CONFIG_PATH -ErrorAction SilentlyContinue
        }
        else {
            $env:CONFIG_PATH = $previousConfigPath
        }
    }

    $verificationRoot = Join-Path "Results\fault_tolerance_verification" $VerificationId
    $baselinePartPath = Join-Path (Join-Path (Join-Path $verificationRoot "without_fault") $runtimeCfg.RunLabel) "part_files"
    $faultPartPath = Join-Path (Join-Path (Join-Path $verificationRoot "with_fault") $runtimeCfg.RunLabel) "part_files"
    $baselineMergedPath = Join-Path (Split-Path -Parent $baselinePartPath) "q1_base.csv"
    $faultMergedPath = Join-Path (Split-Path -Parent $faultPartPath) "q1_base.csv"

    if ((Test-FinalizedPartFiles -Path $baselinePartPath) -and
        (Test-FinalizedPartFiles -Path $faultPartPath)) {
        Write-Host ""
        Write-Host "Confronto end-to-end output senza guasto vs con guasto..."
        python .\scripts\compare_fault_tolerance_outputs.py `
            --baseline-dir $baselinePartPath `
            --fault-dir $faultPartPath `
            --report-json $runtimeCfg.ComparisonReportJson `
            --report-csv $runtimeCfg.ComparisonReportCsv `
            --baseline-merged-output $baselineMergedPath `
            --fault-merged-output $faultMergedPath
        if ($LASTEXITCODE -eq 0) {
            Write-Host "[OK] Nessuna perdita, chiave duplicata o differenza di valore rilevata."
        }
        else {
            $comparisonFailureMessage = "Confronto output fallito. Vedi $($runtimeCfg.ComparisonReportJson)"
            $runFailureMessages += $comparisonFailureMessage
            Write-Host "[FAIL] $comparisonFailureMessage"
        }
    }
    else {
        $missingMode = if ($timingMode -eq "without_fault") { "con guasto" } else { "senza guasto" }
        Write-Host ""
        Write-Host "Output $timingMode salvato. Per completare il confronto esegui la run $missingMode"
        Write-Host "con -VerificationId $VerificationId e lo stesso numero di -Runs."
    }
}
elseif ($NoMerge) {
    Write-Host "Confronto output saltato: -NoMerge misura i tempi ma non dimostra l'equivalenza degli output."
}
else {
    Write-Host "Merge e confronto output saltati a causa di un errore precedente nella run."
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
Write-Host "Run $runIndex/$Runs completata."

if ($runFailureMessages.Count -gt 0) {
    throw ($runFailureMessages -join " ")
}
}

Write-Host ""
Write-Host "Fatto."
