[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Mandatory = $true)]
    [string]$ProjectPath,

    [Parameter(Mandatory = $true)]
    [string]$UvPath,

    [Parameter(Mandatory = $true)]
    [string]$OutputPath,

    [Parameter(Mandatory = $true)]
    [string]$SignatureOutputPath,

    [Parameter(Mandatory = $true)]
    [string]$SigningPrivateKeyPath,

    [Parameter(Mandatory = $true)]
    [string]$SigningPublicKeyPath,

    [ValidatePattern('^$|^[0-9a-f]{64}$')]
    [string]$CampaignHash = '',

    [ValidateRange(1, 168)]
    [int]$MaximumAgeHours = 24,

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
$resolvedSignature = [IO.Path]::GetFullPath($SignatureOutputPath)
if ((Split-Path -Parent $resolvedSignature) -ne $outputDirectory) {
    throw 'The evidence and detached signature must use the same output directory.'
}
if ([IO.Path]::GetExtension($resolvedSignature) -ne '.json') {
    throw 'The detached signature output path must end in .json.'
}
if ((Test-Path -LiteralPath $resolvedSignature) -and -not $Replace) {
    throw 'The detached signature already exists; pass -Replace explicitly.'
}
$resolvedPrivateKey = (Resolve-Path -LiteralPath $SigningPrivateKeyPath).Path
$resolvedPublicKey = (Resolve-Path -LiteralPath $SigningPublicKeyPath).Path
if (-not (Test-Path -LiteralPath $resolvedPrivateKey -PathType Leaf)) {
    throw 'The Ed25519 private key file is missing.'
}
if (-not (Test-Path -LiteralPath $resolvedPublicKey -PathType Leaf)) {
    throw 'The pinned Ed25519 public key file is missing.'
}
if ($resolvedPrivateKey -eq $resolvedPublicKey) {
    throw 'The private and public key files must differ.'
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
        signature_written = $false
        storage_mutation_allowed = $false
        vendor_request_started = $false
        what_if = $true
    } | ConvertTo-Json -Compress
    return
}

Push-Location $resolvedProject
try {
    $exportOutput = @(& $resolvedUv @commandArguments) -join [Environment]::NewLine
    $exportExitCode = $LASTEXITCODE
}
finally {
    Pop-Location
}
try {
    $exportSummary = $exportOutput | ConvertFrom-Json -ErrorAction Stop
}
catch {
    throw 'The readiness exporter returned invalid JSON.'
}
if (
    $exportSummary.artifact_written -ne $true -or
    $exportSummary.live_trading_locked -ne $true -or
    $exportSummary.storage_mutation_allowed -ne $false -or
    $exportSummary.broker_mutation_allowed -ne $false -or
    $exportSummary.vendor_request_started -ne $false -or
    $exportSummary.collection_started -ne $false -or
    -not (Test-Path -LiteralPath $resolvedOutput -PathType Leaf)
) {
    throw 'The readiness exporter did not prove its safety boundary.'
}
if ($exportExitCode -notin @(0, 2)) {
    throw 'The readiness exporter failed.'
}

$signArguments = @(
    'run',
    '--frozen',
    '--no-sync',
    'autoquant',
    'operations-readiness-sign',
    '--input',
    $resolvedOutput,
    '--private-key',
    $resolvedPrivateKey,
    '--signature-output',
    $resolvedSignature
)
if ($Replace) {
    $signArguments += '--replace'
}
Push-Location $resolvedProject
try {
    $signOutput = @(& $resolvedUv @signArguments) -join [Environment]::NewLine
    $signExitCode = $LASTEXITCODE
}
finally {
    Pop-Location
}
try {
    $signSummary = $signOutput | ConvertFrom-Json -ErrorAction Stop
}
catch {
    throw 'The readiness signer returned invalid JSON.'
}
if (
    $signSummary.signature_written -ne $true -or
    $signSummary.live_trading_locked -ne $true -or
    $signSummary.broker_mutation_allowed -ne $false -or
    -not (Test-Path -LiteralPath $resolvedSignature -PathType Leaf)
) {
    throw 'The readiness signer did not prove its safety boundary.'
}
if ($signExitCode -notin @(0, 2) -or $signExitCode -ne $exportExitCode) {
    throw 'The readiness signer failed or returned a conflicting state.'
}

$verifyArguments = @(
    'run',
    '--frozen',
    '--no-sync',
    'autoquant',
    'operations-readiness-verify-signed',
    '--input',
    $resolvedOutput,
    '--signature',
    $resolvedSignature,
    '--public-key',
    $resolvedPublicKey,
    '--max-age-hours',
    $MaximumAgeHours
)
Push-Location $resolvedProject
try {
    $verifyOutput = @(& $resolvedUv @verifyArguments) -join [Environment]::NewLine
    $verifyExitCode = $LASTEXITCODE
}
finally {
    Pop-Location
}
try {
    $verifySummary = $verifyOutput | ConvertFrom-Json -ErrorAction Stop
}
catch {
    throw 'The signed readiness verifier returned invalid JSON.'
}
if (
    $verifySummary.signature_valid -ne $true -or
    $verifySummary.key_id -ne $signSummary.key_id -or
    $verifySummary.report_hash -ne $exportSummary.report_hash -or
    $verifySummary.live_trading_locked -ne $true -or
    $verifySummary.storage_mutation_allowed -ne $false -or
    $verifySummary.broker_mutation_allowed -ne $false -or
    $verifySummary.vendor_request_started -ne $false -or
    $verifySummary.collection_started -ne $false
) {
    throw 'The signed readiness verifier did not prove its safety boundary.'
}
if ($verifyExitCode -notin @(0, 2) -or $verifyExitCode -ne $exportExitCode) {
    throw 'The signed readiness verifier failed or returned a conflicting state.'
}
$verifyOutput
exit $verifyExitCode
