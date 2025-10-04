$pythonScript = Join-Path $PSScriptRoot 'dbt.py'
python $pythonScript @args
exit $LASTEXITCODE
