[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Mandatory = $true)]
    [string]$ProjectPath,

    [Parameter(Mandatory = $true)]
    [string]$UvPath,

    [Parameter(Mandatory = $true)]
    [string]$OutputPath,

    [ValidatePattern('^$|^[0-9a-f]{64}$')]
    [string]$CampaignHash = '',

    [switch]$Replace
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
    throw 'AutoQuant readiness evidence must be exported on the Windows QMT host.'
}

$resolvedProject = (Resolve-Path -LiteralPath $ProjectPath).Path
$resolvedUv = (Resolve-Path -LiteralPath $UvPath).Path
if (-not (Test-Path -LiteralPath $resolvedProject -PathType Container)) {
    throw 'ProjectPath must be an existing directory.'
}
if (-not (Test-Path -LiteralPath $resolvedUv -PathType Leaf)) {
    throw 'UvPath must be an existing executable file.'
}
$envPath = Join-Path $resolvedProject '.env'
if (-not (Test-Path -LiteralPath $envPath -PathType Leaf)) {
    throw 'The project .env file is required and must be provisioned separately.'
}
if (-not (Select-String -LiteralPath $envPath -Quiet -Pattern '^\s*AQ_ENVIRONMENT\s*=\s*paper\s*$')) {
    throw 'The project .env must contain AQ_ENVIRONMENT=paper.'
}
$lockPath = Join-Path $resolvedProject 'uv.lock'
$venvPython = Join-Path $resolvedProject '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $lockPath -PathType Leaf)) {
    throw 'The committed uv.lock file is required.'
}
if (-not (Test-Path -LiteralPath $venvPython -PathType Leaf)) {
    throw 'The Windows virtual environment is missing; run uv sync --frozen first.'
}

$resolvedOutput = [IO.Path]::GetFullPath($OutputPath)
$outputDirectory = Split-Path -Parent $resolvedOutput
if (-not (Test-Path -LiteralPath $outputDirectory -PathType Container)) {
    throw 'The evidence output directory must already exist.'
}
if ([IO.Path]::GetExtension($resolvedOutput) -ne '.json') {
    throw 'The evidence output path must end in .json.'
}
if ((Test-Path -LiteralPath $resolvedOutput) -and -not $Replace) {
    throw 'The evidence output already exists; pass -Replace to replace it explicitly.'
}

$commandArguments = @(
    'run',
    '--frozen',
    '--no-sync',
    'autoquant',
    'operations-readiness-export',
    '--output',
    $resolvedOutput
)
if ($CampaignHash) {
    $commandArguments += @('--campaign-hash', $CampaignHash)
}
if ($Replace) {
    $commandArguments += '--replace'
}

if (-not $PSCmdlet.ShouldProcess($resolvedOutput, 'Export AutoQuant read-only evidence')) {
    [ordered]@{
        artifact_written = $false
        broker_mutation_allowed = $false
        collection_started = $false
        live_trading_locked = $true
        storage_mutation_allowed = $false
        vendor_request_started = $false
        what_if = $true
    } | ConvertTo-Json -Compress
    return
}

Push-Location $resolvedProject
try {
    $rawOutput = @(& $resolvedUv @commandArguments) -join [Environment]::NewLine
    $nativeExitCode = $LASTEXITCODE
}
finally {
    Pop-Location
}
try {
    $summary = $rawOutput | ConvertFrom-Json -ErrorAction Stop
}
catch {
    throw 'The readiness exporter returned invalid JSON.'
}
if (
    $summary.artifact_written -ne $true -or
    $summary.live_trading_locked -ne $true -or
    $summary.storage_mutation_allowed -ne $false -or
    $summary.broker_mutation_allowed -ne $false -or
    $summary.vendor_request_started -ne $false -or
    $summary.collection_started -ne $false -or
    -not (Test-Path -LiteralPath $resolvedOutput -PathType Leaf)
) {
    throw 'The readiness exporter did not prove its safety boundary.'
}
if ($nativeExitCode -notin @(0, 2)) {
    throw 'The readiness exporter failed.'
}
$rawOutput
exit $nativeExitCode
