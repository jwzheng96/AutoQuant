[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Mandatory = $true)]
    [string]$ProjectPath,

    [Parameter(Mandatory = $true)]
    [string]$UvPath,

    [ValidatePattern('^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$')]
    [string]$TaskName = 'AutoQuant-Paper-Watchdog',

    [ValidateRange(1, 60)]
    [int]$IntervalMinutes = 1,

    [switch]$RunAsSystem,

    [switch]$Replace
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
    throw 'AutoQuant paper watchdog task can be installed only on Windows.'
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

$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($null -ne $existing -and -not $Replace) {
    throw "Scheduled task '$TaskName' already exists; pass -Replace to update it."
}

$action = New-ScheduledTaskAction `
    -Execute $resolvedUv `
    -Argument 'run --frozen --no-sync autoquant paper-watchdog-enforce' `
    -WorkingDirectory $resolvedProject
$trigger = New-ScheduledTaskTrigger `
    -At (Get-Date).AddMinutes(1) `
    -Once `
    -RepetitionDuration (New-TimeSpan -Days 3650) `
    -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes)
if ($RunAsSystem) {
    $principal = New-ScheduledTaskPrincipal `
        -UserId 'SYSTEM' `
        -LogonType ServiceAccount `
        -RunLevel Highest
    $identity = 'SYSTEM'
}
else {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
    $principal = New-ScheduledTaskPrincipal `
        -UserId $identity `
        -LogonType Interactive `
        -RunLevel Limited
}
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 2) `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable
$task = New-ScheduledTask `
    -Action $action `
    -Description 'Independent AutoQuant watchdog; it can only re-arm the paper kill switch.' `
    -Principal $principal `
    -Settings $settings `
    -Trigger $trigger

$registered = $false
if ($PSCmdlet.ShouldProcess($TaskName, 'Register AutoQuant paper watchdog task')) {
    Register-ScheduledTask `
        -Force:$Replace `
        -InputObject $task `
        -TaskName $TaskName | Out-Null
    $registered = $true
}

[ordered]@{
    command = 'uv run --frozen --no-sync autoquant paper-watchdog-enforce'
    installed = $registered
    interval_minutes = $IntervalMinutes
    live_trading_locked = $true
    run_as = $identity
    secrets_in_task_arguments = $false
    started = $false
    task_name = $TaskName
} | ConvertTo-Json -Compress
