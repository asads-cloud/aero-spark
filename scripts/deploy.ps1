#requires -Version 7
<#
.SYNOPSIS
Deploy Airflow DAGs and requirements.txt to MWAA S3 prefixes.
#>

param(
  [Parameter(Mandatory=$true)] [string] $Bucket,
  [string] $DagsPrefix = "dags",
  [string] $RequirementsPrefix = "requirements",
  [string] $MwaaEnv = "",
  [string] $Profile = "aero-admin",
  [string] $Region  = "eu-west-1",
  [switch] $UpdateMwaa
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Test-Command($Name) {
  $exists = Get-Command $Name -ErrorAction SilentlyContinue
  if (-not $exists) { throw "Required command '$Name' not found in PATH." }
}

Write-Host "==> Validating prerequisites..." -ForegroundColor Cyan
Test-Command "aws"
Test-Command "git"

$repoRoot = (git rev-parse --show-toplevel).Trim()
Set-Location $repoRoot

$localDags = Join-Path $repoRoot "airflow\dags"
$localReq  = Join-Path $repoRoot "airflow\requirements.txt"

if (-not (Test-Path $localDags)) { throw "Local DAGs path not found: $localDags" }
if (-not (Test-Path $localReq))  { throw "Local requirements.txt not found: $localReq" }

$dagsUri = "s3://$Bucket/$DagsPrefix/"
$reqUri  = "s3://$Bucket/$RequirementsPrefix/requirements.txt"

Write-Host "==> Deploying DAGs to $dagsUri" -ForegroundColor Cyan
aws --profile $Profile --region $Region s3 sync $localDags $dagsUri --delete

Write-Host "==> Uploading requirements.txt to $reqUri" -ForegroundColor Cyan
aws --profile $Profile --region $Region s3 cp $localReq $reqUri

if ($UpdateMwaa -and $MwaaEnv) {
  Write-Host "==> Triggering MWAA update for environment '$MwaaEnv'..." -ForegroundColor Cyan
  aws --profile $Profile --region $Region mwaa update-environment `
    --name $MwaaEnv `
    --requirements-s3-path "$RequirementsPrefix/requirements.txt" `
    --dags-s3-path "$DagsPrefix/" | Out-Null
  Write-Host "   - Update requested. Use 'aws mwaa get-environment --name $MwaaEnv' to watch status."
}

Write-Host "`n==> Deployment summary" -ForegroundColor Green
Write-Host "   DAGs: $dagsUri"
Write-Host "   Requirements: $reqUri"
if ($MwaaEnv) { Write-Host "   MWAA env: $MwaaEnv" }
Write-Host "Done."