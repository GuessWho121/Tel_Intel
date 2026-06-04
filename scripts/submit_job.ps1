param(
    [parameter(Mandatory=$true)]
    [string]$JobPath
)

$ContainerRoot = "/opt/airtel/Sproj"
$ContainerJobPath = "$ContainerRoot/$JobPath"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$EnvPath = Join-Path $ProjectRoot ".env"

if (Test-Path $EnvPath) {
    Get-Content $EnvPath | ForEach-Object {
        if ($_ -and -not $_.StartsWith("#")) {
            $name, $value = $_ -split "=", 2
            [Environment]::SetEnvironmentVariable($name.Trim(), $value.Trim(), "Process")
        }
    }
}

docker exec `
    -e PYTHONPATH="$ContainerRoot/src" `
    -e MINIO_ACCESS_KEY=$env:MINIO_ACCESS_KEY `
    -e MINIO_SECRET_KEY=$env:MINIO_SECRET_KEY `
    sparkmaster `
    /opt/spark/bin/spark-submit `
    --master spark://100.71.201.92:7077 `
    --conf spark.driver.host=100.71.201.92 `
    --conf spark.driver.port=10000 `
    --conf spark.driver.bindAddress=0.0.0.0 `
    --conf spark.blockManager.port=10001 `
    $ContainerJobPath
