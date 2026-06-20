<#
.SYNOPSIS
  Load-tests both create implementations and compares their throughput, then reconciles the
  number of rows committed to PostgreSQL against the number of events committed to Kafka to prove
  each implementation is atomic (db count == kafka count == HTTP 201s, even when requests fail).

.DESCRIPTION
  Prerequisites (run these first, in separate terminals):
    1. docker compose up -d
    2. ./mvnw package
    3. java -jar target/quarkus-app/quarkus-run.jar    (prod profile -> connects to the compose stack)

  k6 runs via Docker (grafana/k6), so no local k6 install is needed. The app is reached on the
  host via host.docker.internal.

.EXAMPLE
  ./load-test/run-comparison.ps1 -Vus 50 -Duration 30s -InvalidRatio 0.1
#>
param(
  [int]$Vus = 30,
  [string]$Duration = '30s',
  [double]$InvalidRatio = 0.1,
  [string[]]$Endpoints = @('/items/pooled', '/items/chained'),
  # Use a locally installed k6 binary (targets localhost:8080). Without this, k6 runs via Docker
  # (grafana/k6) and reaches the app at host.docker.internal:8080.
  [switch]$UseLocalK6,
  [string]$BaseUrlForK6 = 'http://host.docker.internal:8080',
  [string]$Topic = 'item-created',
  [int]$Partitions = 12,
  [string]$PgContainer = 'kdt-postgres',
  [string]$KafkaContainer = 'kdt-kafka'
)

# Continue (not Stop): native docker/kafka commands write to stderr (e.g. deleting a
# not-yet-existing topic, or the console consumer's expected timeout). Under 'Stop' those wrapped
# stderr lines terminate the script. Explicit checks + throws below handle the real failures.
$ErrorActionPreference = 'Continue'
$scriptPath = Join-Path $PSScriptRoot 'item-load-test.js'

function Test-AppUp {
  return (Test-NetConnection -ComputerName localhost -Port 8080 -InformationLevel Quiet -WarningAction SilentlyContinue)
}

function Reset-State {
  # Clean DB baseline.
  docker exec $PgContainer psql -U quarkus -d items -c "TRUNCATE TABLE items;" *>$null
  # Fresh topic so committed-record counts are absolute for this run.
  docker exec $KafkaContainer /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $Topic *>$null
  for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Milliseconds 500
    docker exec $KafkaContainer /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 `
      --create --topic $Topic --partitions $Partitions --replication-factor 1 *>$null
    if ($LASTEXITCODE -eq 0) { return }
  }
  throw "Could not (re)create topic '$Topic' - is the Kafka container '$KafkaContainer' running?"
}

function Get-DbCount {
  $out = docker exec $PgContainer psql -U quarkus -d items -t -A -c "SELECT count(*) FROM items;"
  return [int](($out | Select-Object -Last 1).Trim())
}

function Get-KafkaCount {
  # Count COMMITTED data records only (read_committed hides aborted records and tx markers).
  $lines = docker exec $KafkaContainer /opt/kafka/bin/kafka-console-consumer.sh `
    --bootstrap-server localhost:9092 --topic $Topic --from-beginning `
    --isolation-level read_committed --timeout-ms 10000 2>$null
  if (-not $lines) { return 0 }
  return ($lines | Where-Object { $_ -ne '' } | Measure-Object).Count
}

function Invoke-K6 {
  param([string]$Endpoint)
  if ($UseLocalK6) {
    # Locally installed k6 binary; targets the app directly on localhost.
    $k6Args = @(
      'run', '--quiet',
      '-e', "BASE_URL=http://localhost:8080",
      '-e', "ENDPOINT=$Endpoint",
      '-e', "VUS=$Vus",
      '-e', "DURATION=$Duration",
      '-e', "INVALID_RATIO=$InvalidRatio",
      $scriptPath
    )
    $raw = & k6 @k6Args 2>$null
    $reach = 'http://localhost:8080'
  }
  else {
    # k6 via Docker; reaches the host app at host.docker.internal. Script is piped over stdin.
    $dockerArgs = @(
      'run', '--rm', '-i', '--add-host=host.docker.internal:host-gateway',
      '-e', "BASE_URL=$BaseUrlForK6",
      '-e', "ENDPOINT=$Endpoint",
      '-e', "VUS=$Vus",
      '-e', "DURATION=$Duration",
      '-e', "INVALID_RATIO=$InvalidRatio",
      'grafana/k6', 'run', '--quiet', '-'
    )
    $raw = Get-Content $scriptPath -Raw | docker @dockerArgs 2>$null
    $reach = $BaseUrlForK6
  }
  $jsonLine = $raw | Where-Object { $_.TrimStart().StartsWith('{') } | Select-Object -Last 1
  if (-not $jsonLine) { throw "k6 produced no summary - is the app reachable at $reach ?" }
  return $jsonLine | ConvertFrom-Json
}

if (-not (Test-AppUp)) {
  throw "App is not listening on localhost:8080. Start it with: java -jar target/quarkus-app/quarkus-run.jar"
}

Write-Host "Load test: VUs=$Vus Duration=$Duration InvalidRatio=$InvalidRatio" -ForegroundColor Cyan
$results = @()

foreach ($ep in $Endpoints) {
  Write-Host "`n--- $ep ---" -ForegroundColor Cyan
  Reset-State
  $k = Invoke-K6 -Endpoint $ep

  $dbCount = Get-DbCount
  $kafkaCount = Get-KafkaCount
  $created = [int]$k.items_created
  $failed = [int]$k.items_failed
  $durS = [double]$k.duration_s
  $createsPerSec = if ($durS -gt 0) { [math]::Round($created / $durS, 1) } else { 0 }
  $consistent = ($dbCount -eq $kafkaCount) -and ($dbCount -eq $created)

  Write-Host ("  201 created={0}  failed={1}  db rows={2}  kafka msgs={3}  consistent={4}" -f `
      $created, $failed, $dbCount, $kafkaCount, $consistent) -ForegroundColor ($(if ($consistent) { 'Green' } else { 'Red' }))

  $results += [pscustomobject][ordered]@{
    Endpoint    = $ep
    'Creates/s' = $createsPerSec
    'Reqs/s'    = [math]::Round([double]$k.http_reqs_per_s, 1)
    'p95 ms'    = [math]::Round([double]$k.p95_ms, 1)
    Created     = $created
    Failed      = $failed
    DbRows      = $dbCount
    KafkaMsgs   = $kafkaCount
    Consistent  = $consistent
  }
}

Write-Host "`n================ THROUGHPUT COMPARISON ================" -ForegroundColor Cyan
$results | Format-Table -AutoSize

$allConsistent = ($results | Where-Object { -not $_.Consistent } | Measure-Object).Count -eq 0
if ($allConsistent) {
  Write-Host "PASS: for every implementation DB rows == Kafka msgs == 201s (atomic; failures rolled back both sides)." -ForegroundColor Green
} else {
  Write-Host "FAIL: a DB/Kafka count mismatch was detected - a partial commit occurred." -ForegroundColor Red
  exit 1
}
