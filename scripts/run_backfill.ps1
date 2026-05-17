param(
    [Parameter(Mandatory = $true)]
    [string]$StartDate
)

$ErrorActionPreference = "Stop"

$ProjectRoot = "C:\dev\canadian-weather-data-pipeline"
$DuckDbPath = "$ProjectRoot\data\warehouse\weather.duckdb"
$EndDate = Get-Date -Format "yyyy-MM-dd"

Write-Host "Starting historical backfill..."
Write-Host "Start date: $StartDate"
Write-Host "End date:   $EndDate"

Set-Location $ProjectRoot

Write-Host "Activating virtual environment..."
& "$ProjectRoot\.venv-1\Scripts\Activate.ps1"

Write-Host "Running historical weather backfill..."
python src/backfill_weather_history.py --start-date $StartDate --end-date $EndDate

Write-Host "Loading Parquet files into DuckDB..."
python src/load_duckdb.py

Write-Host "Running dbt full refresh..."
Set-Location "$ProjectRoot\dbt_weather"
$env:DBT_DUCKDB_PATH = $DuckDbPath

dbt deps
dbt build --full-refresh

Write-Host "Backfill completed successfully."