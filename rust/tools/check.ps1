# Offline/loopback only. No live exchange calls or environment credential loading.
param([switch]$Format)
$ErrorActionPreference = 'Stop'
$rustRoot = Split-Path $PSScriptRoot -Parent
$cargoCommand = Get-Command cargo -ErrorAction SilentlyContinue
if ($null -ne $cargoCommand) {
    $cargoExe = $cargoCommand.Source
} else {
    $cargoExe = Join-Path $env:USERPROFILE '.cargo/bin/cargo.exe'
    if (-not (Test-Path -LiteralPath $cargoExe)) {
        throw 'Rust/Cargo is not installed. Install the official Rust MSVC toolchain first.'
    }
}
$manifest = Join-Path $rustRoot 'Cargo.toml'
if ($Format) {
    & $cargoExe fmt --manifest-path $manifest --all
    if ($LASTEXITCODE -ne 0) { throw 'cargo fmt failed' }
}
& $cargoExe fmt --manifest-path $manifest --all -- --check
if ($LASTEXITCODE -ne 0) { throw 'Formatting check failed; run this script with -Format' }
& $cargoExe test --manifest-path $manifest --workspace --locked
if ($LASTEXITCODE -ne 0) { throw 'Rust tests failed' }
& $cargoExe clippy --manifest-path $manifest --workspace --all-targets --locked -- -D warnings
if ($LASTEXITCODE -ne 0) { throw 'Clippy failed' }
& $cargoExe build --manifest-path $manifest --workspace --release --locked
if ($LASTEXITCODE -ne 0) { throw 'Rust release build failed' }
& python -m unittest discover -s $PSScriptRoot -p 'test_*.py'
if ($LASTEXITCODE -ne 0) { throw 'Parity verifier tests failed' }
& python (Join-Path $PSScriptRoot 'verify_parity.py') --benchmark
if ($LASTEXITCODE -ne 0) { throw 'Python/Rust parity failed' }
& python (Join-Path $PSScriptRoot 'verify_market.py')
if ($LASTEXITCODE -ne 0) { throw 'Public market replay parity failed' }
& python (Join-Path $PSScriptRoot 'verify_shared.py')
if ($LASTEXITCODE -ne 0) { throw 'Shared public market parity failed' }
& python (Join-Path $PSScriptRoot 'verify_instruments.py')
if ($LASTEXITCODE -ne 0) { throw 'Instrument and symbol parity failed' }
& python (Join-Path $PSScriptRoot 'verify_private.py')
if ($LASTEXITCODE -ne 0) { throw 'Private event replay parity failed' }
& python (Join-Path $PSScriptRoot 'verify_aster_auth.py')
if ($LASTEXITCODE -ne 0) { throw 'Aster authentication fixture parity failed' }
& python (Join-Path $PSScriptRoot 'verify_hl_trade_signing.py')
if ($LASTEXITCODE -ne 0) { throw 'Hyperliquid trading signature parity failed' }
& python (Join-Path $PSScriptRoot 'verify_hyperliquid_private.py')
if ($LASTEXITCODE -ne 0) { throw 'Hyperliquid private order-status parity failed' }
& python (Join-Path $PSScriptRoot 'verify_execution.py')
if ($LASTEXITCODE -ne 0) { throw 'Execution residual retry parity failed' }
& python (Join-Path $PSScriptRoot 'inventory.py') --check
if ($LASTEXITCODE -ne 0) { throw 'Source inventory drifted' }
Write-Output 'Core, partial public-market and private-event offline validation passed. This does not authorize live trading.'
