[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Mandatory = $true)]
    [string]$ProjectPath,

    [Parameter(Mandatory = $true)]
    [string]$UvPath,

    [ValidatePattern('^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$')]
    [string]$TaskName = 'AutoQuant-Paper-Runtime',

    [ValidateRange(0, 30)]
    [int]$StartupDelayMinutes = 2,

    [switch]$Replace
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
    throw 'AutoQuant paper runtime task can be installed only on Windows.'
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

$identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
$action = New-ScheduledTaskAction `
    -Execute $resolvedUv `
    -Argument 'run --frozen --no-sync autoquant run-paper' `
    -WorkingDirectory $resolvedProject
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $identity
if ($StartupDelayMinutes -gt 0) {
    $trigger.Delay = "PT${StartupDelayMinutes}M"
}
$principal = New-ScheduledTaskPrincipal `
    -UserId $identity `
    -LogonType Interactive `
    -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -MultipleInstances IgnoreNew `
    -RestartCount 999 `
    -RestartInterval (New-TimeSpan -Minutes 1)
$task = New-ScheduledTask `
    -Action $action `
    -Description 'AutoQuant paper-only runtime; live broker mutations remain hard-locked.' `
    -Principal $principal `
    -Settings $settings `
    -Trigger $trigger

$registered = $false
if ($PSCmdlet.ShouldProcess($TaskName, 'Register AutoQuant paper runtime task')) {
    Register-ScheduledTask `
        -Force:$Replace `
        -InputObject $task `
        -TaskName $TaskName | Out-Null
    $registered = $true
}

[ordered]@{
    command = 'uv run --frozen --no-sync autoquant run-paper'
    installed = $registered
    live_trading_locked = $true
    secrets_in_task_arguments = $false
    started = $false
    startup_delay_minutes = $StartupDelayMinutes
    task_name = $TaskName
    user = $identity
} | ConvertTo-Json -Compress
