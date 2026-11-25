param(
    [Parameter(Mandatory = $true)]
    [string]$Archivo,
    [string]$Perfil,
    [string]$Cli = "databricks"
)

$ErrorActionPreference = "Stop"
$fuente = Get-Item -LiteralPath $Archivo
if ($fuente.PSIsContainer -or $fuente.Name -cnotmatch '^AIS_\d{4}_\d{2}_\d{2}(?:_[A-Za-z0-9-]{1,32})?\.(zip|csv)$') {
    throw "Use un archivo AIS_AAAA_MM_DD.zip o .csv, con un sufijo opcional de lote."
}
$comando = (Get-Command $Cli -CommandType Application).Source
$autenticacion = @()
if ($Perfil) { $autenticacion += @("--profile", $Perfil) }

Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    $bundle = & $comando bundle validate -o json @autenticacion
    if ($LASTEXITCODE -ne 0) { throw "No se pudo resolver la configuración del Job." }
    $job = ($bundle | ConvertFrom-Json).resources.jobs.oceanwatch_pipeline_entrega2
    $inbox = $job.trigger.file_arrival.url.TrimEnd("/")

    & $comando fs cp $fuente.FullName "dbfs:${inbox}/$($fuente.Name)" @autenticacion
    if ($LASTEXITCODE -ne 0) { throw "La carga falló. Se conservan los archivos que ya estaban en la carpeta." }

    & $comando bundle run oceanwatch_pipeline_entrega2 --no-wait @autenticacion
    if ($LASTEXITCODE -ne 0) { throw "El archivo está cargado. Use Run now para procesarlo." }
}
finally {
    Pop-Location
}
