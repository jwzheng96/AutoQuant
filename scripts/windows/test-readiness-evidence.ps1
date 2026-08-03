[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
    throw 'AutoQuant Windows script acceptance must run on Windows.'
}

$scriptsRoot = $PSScriptRoot
$parseFailures = @()
foreach ($script in Get-ChildItem -LiteralPath $scriptsRoot -Filter '*.ps1' -File) {
    $tokens = $null
    $errors = $null
    [void][Management.Automation.Language.Parser]::ParseFile(
        $script.FullName,
        [ref]$tokens,
        [ref]$errors
    )
    foreach ($error in $errors) {
        $parseFailures += "$($script.Name):$($error.Extent.StartLineNumber):$($error.Message)"
    }
}
if ($parseFailures.Count -ne 0) {
    throw "PowerShell parsing failed: $($parseFailures -join '; ')"
}

$temporaryRoot = Join-Path `
    ([IO.Path]::GetTempPath()) `
    "autoquant-windows-readiness-$([Guid]::NewGuid().ToString('N'))"
$projectPath = Join-Path $temporaryRoot 'project'
$scriptsPath = Join-Path $projectPath '.venv\Scripts'
$evidencePath = Join-Path $temporaryRoot 'evidence'
$keysPath = Join-Path $temporaryRoot 'keys'
try {
    [void](New-Item -ItemType Directory -Path $scriptsPath -Force)
    [void](New-Item -ItemType Directory -Path $evidencePath -Force)
    [void](New-Item -ItemType Directory -Path $keysPath -Force)
    Set-Content `
        -LiteralPath (Join-Path $projectPath '.env') `
        -Value 'AQ_ENVIRONMENT=paper' `
        -Encoding utf8NoBOM
    Set-Content `
        -LiteralPath (Join-Path $projectPath 'uv.lock') `
        -Value 'acceptance-placeholder' `
        -Encoding utf8NoBOM
    Set-Content `
        -LiteralPath (Join-Path $scriptsPath 'python.exe') `
        -Value 'must-not-execute' `
        -Encoding ascii
    $uvPath = Join-Path $temporaryRoot 'uv.exe'
    Set-Content -LiteralPath $uvPath -Value 'must-not-execute' -Encoding ascii
    $privateKey = Join-Path $keysPath 'readiness.private.pem'
    $publicKey = Join-Path $keysPath 'readiness.public.pem'
    Set-Content -LiteralPath $privateKey -Value 'must-not-read' -Encoding ascii
    Set-Content -LiteralPath $publicKey -Value 'must-not-read' -Encoding ascii
    $artifactPath = Join-Path $evidencePath 'readiness.json'
    $signaturePath = Join-Path $evidencePath 'readiness.sig.json'

    $result = @(
        & (Join-Path $scriptsRoot 'export-readiness-evidence.ps1') `
            -ProjectPath $projectPath `
            -UvPath $uvPath `
            -OutputPath $artifactPath `
            -SignatureOutputPath $signaturePath `
            -SigningPrivateKeyPath $privateKey `
            -SigningPublicKeyPath $publicKey `
            -WhatIf 6>&1
    )
    $jsonLine = $result |
        ForEach-Object { $_.ToString() } |
        Where-Object { $_ -match '^\{"artifact_written"' } |
        Select-Object -Last 1
    if (-not $jsonLine) {
        throw 'The readiness WhatIf run did not emit its safety summary.'
    }
    $summary = $jsonLine | ConvertFrom-Json -ErrorAction Stop
    if (
        $summary.what_if -ne $true -or
        $summary.artifact_written -ne $false -or
        $summary.signature_written -ne $false -or
        $summary.live_trading_locked -ne $true -or
        $summary.storage_mutation_allowed -ne $false -or
        $summary.broker_mutation_allowed -ne $false -or
        $summary.vendor_request_started -ne $false -or
        $summary.collection_started -ne $false
    ) {
        throw 'The readiness WhatIf safety summary is invalid.'
    }
    if (
        (Test-Path -LiteralPath $artifactPath) -or
        (Test-Path -LiteralPath $signaturePath)
    ) {
        throw 'The readiness WhatIf run created an evidence artifact.'
    }

    [ordered]@{
        artifact_created = $false
        broker_mutation_allowed = $false
        collection_started = $false
        live_trading_locked = $true
        parsed_script_count = (Get-ChildItem -LiteralPath $scriptsRoot -Filter '*.ps1' -File).Count
        signature_created = $false
        status = 'ok'
        storage_mutation_allowed = $false
        uv_executed = $false
        vendor_request_started = $false
    } | ConvertTo-Json -Compress
}
finally {
    if (Test-Path -LiteralPath $temporaryRoot) {
        Remove-Item -LiteralPath $temporaryRoot -Recurse -Force
    }
}
