param(
    [Parameter(Mandatory = $true)]
    [string]$Archivo,
    [string]$Perfil,
    [string]$Cli = "databricks"
)

$ErrorActionPreference = "Stop"
$fuente = Get-Item -LiteralPath $Archivo
if ($fuente.PSIsContainer -or $fuente.Name -cnotmatch '^(ais-[0-9]{4}-[0-9]{2}-[0-9]{2}\.csv\.zst|AIS_[0-9]{4}_[0-9]{2}_[0-9]{2}(?:_[A-Za-z0-9-]{1,32})?\.(zip|csv))$') {
    throw "Use ais-AAAA-MM-DD.csv.zst, AIS_AAAA_MM_DD.zip o AIS_AAAA_MM_DD.csv."
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

    Write-Output "Archivo cargado. El detector de OceanWatch iniciará el pipeline."
}
finally {
    Pop-Location
}
