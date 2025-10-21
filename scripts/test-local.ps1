param(
    [string]$Venv = ".\venv"
)

Write-Host "==> Ensuring dev deps..."
if (Test-Path $Venv) {
    & "$Venv\Scripts\python.exe" -m pip install -r requirements-dev.txt
} else {
    python -m pip install -r requirements-dev.txt
}

# Point Spark workers to the right python.exe (your venv if present)
$py = if (Test-Path $Venv) { (Resolve-Path "$Venv\Scripts\python.exe").Path } else { (Get-Command python).Source }
$env:PYSPARK_PYTHON = $py
$env:PYSPARK_DRIVER_PYTHON = $py
Write-Host "==> Using Python for Spark:" $py

Write-Host "==> Running pytest..."
pytest -q databricks/tests/test_transform.py
