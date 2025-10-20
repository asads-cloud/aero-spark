param(
  [string]$Bucket      = "aero-spark-data-uc3pslda",
  [string]$Dataset     = "flight_delays",
  [string]$SourceDir   = "data/samples",
  [string]$Profile     = "aero-admin",
  [string]$Region      = "eu-west-1",
  [string]$IngestDate,            # default: today in local time (Europe/London)
  [switch]$DryRun
)

function Fail($msg) { Write-Error $msg; exit 1 }

# 1) Resolve ingest date (YYYY-MM-DD)
if (-not $IngestDate -or $IngestDate.Trim() -eq "") {
  $IngestDate = (Get-Date).ToString("yyyy-MM-dd")
}

# 2) Validate pre-reqs
if (-not (Get-Command aws -ErrorAction SilentlyContinue)) { Fail "AWS CLI not found on PATH." }
$who = aws sts get-caller-identity --profile $Profile 2>$null | ConvertFrom-Json
if (-not $who) { Fail "Cannot use AWS profile '$Profile'. Try 'aws configure --profile $Profile'." }

if (-not (Test-Path $SourceDir)) { Fail "SourceDir '$SourceDir' not found." }

# Pick CSVs
$csvs = Get-ChildItem -Path $SourceDir -Filter *.csv -File -Recurse
if (-not $csvs -or $csvs.Count -eq 0) { Fail "No CSV files found under '$SourceDir'." }

# 3) Quick header sanity: expect 27 columns
$bad = @()
foreach ($f in $csvs) {
  $header = (Get-Content $f.FullName -TotalCount 1)
  $count  = ($header.Split(",")).Count
  if ($count -ne 27) { $bad += [pscustomobject]@{ File=$f.FullName; Columns=$count } }
}
if ($bad.Count -gt 0) {
  $bad | ForEach-Object { Write-Error "Header column count = $($_.Columns) in '$($_.File)' (expected 27)." }
  Fail "One or more CSVs failed header check."
}

# 4) Destination prefix
$dest = "s3://$Bucket/raw/$Dataset/ingest_date=$IngestDate/"

Write-Host "Profile : $Profile"
Write-Host "Region  : $Region"
Write-Host "Bucket  : $Bucket"
Write-Host "Source  : $SourceDir"
Write-Host "Dest    : $dest"
Write-Host "Files   : $($csvs.Count)"
Write-Host "Mode    : " + ($(if($DryRun){"DRY-RUN"} else {"APPLY"}))

# 5) Upload (sync for idempotency)
$syncArgs = @(
  "s3","sync",$SourceDir,$dest,
  "--exclude","*","--include","*.csv",
  "--profile",$Profile,"--region",$Region
)

if ($DryRun) { $syncArgs += "--dryrun" }

Write-Host "`naws $($syncArgs -join ' ')`n"
$rc = & aws @syncArgs
if ($LASTEXITCODE -ne 0) { Fail "aws s3 sync failed." }

# 6) Post-list for confirmation
aws s3 ls $dest --profile $Profile --region $Region

Write-Host "`nSeed complete."
