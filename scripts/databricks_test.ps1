# --- EDIT ---
$jobId  = 731358419893690
$env:DATABRICKS_CONFIG_PROFILE = "dbx-aero"
$runDate = "2025-10-20"         # or compute dynamically
# ------------

# S3 paths (output to bucket)
$input  = "dbfs:/Volumes/workspace/scratch/aero_scratch/flight_delays_sample.csv"
$output = "s3://aero-spark-data-uc3pslda/processed/flight_delays/"

$runBody = @{
  job_id = [int64]$jobId
  idempotency_token = "run-"+[guid]::NewGuid().ToString("N")
  python_params = @(
    "--input_path",  $input,
    "--output_path", $output,
    "--run_date",    $runDate
  )
} | ConvertTo-Json -Depth 6

$run = databricks jobs run-now --json $runBody -o json | ConvertFrom-Json
"Run URL: $($run.run_page_url)"
