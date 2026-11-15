param(
    [Parameter(Mandatory = $true)][string]$PythonExe,
    [Parameter(Mandatory = $true)][string]$RunnerPath,
    [Parameter(Mandatory = $true)][string]$RunnerSha256,
    [Parameter(Mandatory = $true)][string]$CliSha256,
    [Parameter(Mandatory = $true)][string]$GuardSha256,
    [Parameter(Mandatory = $true)][string]$LayoutSha256,
    [Parameter(Mandatory = $true)][string]$QueueDatabase,
    [Parameter(Mandatory = $true)][string]$Log,
    [Parameter(Mandatory = $true)][string]$ReleaseManifest,
    [Parameter(Mandatory = $true)][string]$ReleaseManifestSha256,
    [string]$RemoteConfig,
    [string]$RemoteConfigSha256,
    [switch]$PartialObservation,
    [string]$HistoryRegistry,
    [string]$HistoryRegistrySha256,
    [string]$HistoryAccess,
    [string]$HistoryAccessSha256,
    [string]$HistoryUnreadScript,
    [string]$HistoryUnreadScriptSha256,
    [string]$GuardBundle,
    [string]$GuardBundleSha256,
    [ValidateRange(1, 65535)][int]$Port = 14206,
    [switch]$ClosedProjection
)

$ErrorActionPreference = 'Stop'
if ($Port -eq 14205) { throw 'The postprocessing observer cannot replace the upstream API.' }
foreach ($digest in @($RunnerSha256, $CliSha256, $GuardSha256, $LayoutSha256, $ReleaseManifestSha256)) {
    if ($digest -cnotmatch '^[0-9a-f]{64}$') { throw 'An independently pinned lowercase SHA256 is required.' }
}
$historyValues = @($HistoryRegistry, $HistoryRegistrySha256, $HistoryAccess, $HistoryAccessSha256,
    $HistoryUnreadScript, $HistoryUnreadScriptSha256, $GuardBundle, $GuardBundleSha256)
if (@($historyValues | Where-Object { $_ }).Count -gt 0) {
    if ($ClosedProjection -or @($historyValues | Where-Object { -not $_ }).Count -gt 0 -or
        -not [System.IO.Path]::IsPathRooted($HistoryRegistry) -or
        -not [System.IO.Path]::IsPathRooted($HistoryAccess) -or
        -not [System.IO.Path]::IsPathRooted($GuardBundle) -or
        -not $HistoryUnreadScript.StartsWith('/') -or
        $HistoryRegistrySha256 -cnotmatch '^[0-9a-f]{64}$' -or
        $HistoryAccessSha256 -cnotmatch '^[0-9a-f]{64}$' -or
        $GuardBundleSha256 -cnotmatch '^[0-9a-f]{64}$' -or
        $HistoryUnreadScriptSha256 -cnotmatch '^[0-9a-f]{64}$') {
        throw 'Complete independently pinned history and access options are required.'
    }
}
foreach ($path in @($PythonExe, $RunnerPath, $QueueDatabase, $Log, $ReleaseManifest)) {
    if (-not [System.IO.Path]::IsPathRooted($path)) { throw 'Absolute paths are required.' }
}
if (-not $ClosedProjection) {
    if (-not $RemoteConfig -or -not [System.IO.Path]::IsPathRooted($RemoteConfig) -or
        $RemoteConfigSha256 -cnotmatch '^[0-9a-f]{64}$') {
        throw 'A pinned remote configuration is required.'
    }
}

# Execute the exact bytes read and hashed here. Merely hashing a path before
# asking a second process to reopen it would leave a source replacement gap.
$bootstrap = @'
import hashlib, hmac, sys
from pathlib import Path
path = Path(sys.argv[1]).resolve(strict=True)
expected = sys.argv[2]
raw = path.read_bytes()
if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), expected):
    raise SystemExit('runner source differs from independent hash')
sys.argv = [str(path), *sys.argv[3:]]
exec(compile(raw, str(path), 'exec', dont_inherit=True), {'__name__': '__main__', '__file__': str(path)})
'@
$serviceArguments = @('--log', $Log, '--cli-sha256', $CliSha256, '--', 'serve', '--db', $QueueDatabase,
    '--port', [string]$Port, '--release-manifest', $ReleaseManifest,
    '--release-manifest-sha256', $ReleaseManifestSha256, '--guard-sha256', $GuardSha256,
    '--layout-sha256', $LayoutSha256)
if ($ClosedProjection) {
    $serviceArguments += '--closed-projection'
} else {
    $serviceArguments += @('--remote-config', $RemoteConfig, '--remote-config-sha256', $RemoteConfigSha256)
}
if ($PartialObservation) { $serviceArguments += '--partial-observation' }
if ($HistoryRegistry) {
    $serviceArguments += @('--history-registry', $HistoryRegistry, '--history-registry-sha256', $HistoryRegistrySha256,
        '--history-access', $HistoryAccess, '--history-access-sha256', $HistoryAccessSha256,
        '--history-unread-script', $HistoryUnreadScript, '--history-unread-script-sha256', $HistoryUnreadScriptSha256,
        '--guard-bundle', $GuardBundle, '--guard-bundle-sha256', $GuardBundleSha256)
}

function Quote-WindowsArgument([string]$value) {
    # CommandLineToArgvW quoting, including backslashes before a quote/end.
    $quoted = '"'
    $slashes = 0
    foreach ($character in $value.ToCharArray()) {
        if ($character -eq '\') {
            $slashes += 1
        } elseif ($character -eq '"') {
            $quoted += ('\' * (2 * $slashes + 1)) + '"'
            $slashes = 0
        } else {
            $quoted += ('\' * $slashes) + $character
            $slashes = 0
        }
    }
    return $quoted + ('\' * (2 * $slashes)) + '"'
}

# Encode the fixed bootstrap as one command argument. WaitForExit is required:
# PowerShell's direct invocation of a GUI pythonw.exe can return immediately.
$encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($bootstrap))
$oneLineBootstrap = "import base64;exec(base64.b64decode('$encoded'))"
$pythonArguments = @('-X', 'utf8', '-c', $oneLineBootstrap, $RunnerPath, $RunnerSha256) + $serviceArguments
$startInfo = New-Object System.Diagnostics.ProcessStartInfo
$startInfo.FileName = $PythonExe
$startInfo.Arguments = (($pythonArguments | ForEach-Object { Quote-WindowsArgument ([string]$_) }) -join ' ')
$startInfo.UseShellExecute = $false
$startInfo.CreateNoWindow = $true
$startInfo.WindowStyle = [System.Diagnostics.ProcessWindowStyle]::Hidden
$process = New-Object System.Diagnostics.Process
$process.StartInfo = $startInfo
if (-not $process.Start()) { throw 'Python process could not start.' }
$process.WaitForExit()
exit $process.ExitCode
