$ErrorActionPreference = 'Stop'
$release = 'F:\论文预处理系统\预处理系统2\candidates\postprocess-prefect-20261031\server_release_next'
$launcherPath = Join-Path $release 'start_verified_page.ps1'
$expectedLauncherSha = '59011d24edf63a4388c4919193d0b802cafa195f35119fe8564098ef8cde72ed'
$launchParameters = @{
    PythonExe = 'F:\论文预处理系统\预处理系统2\prefect-first-step-independent\.venv\Scripts\pythonw.exe'
    RunnerPath = (Join-Path $release 'run_waiting_service.py')
    RunnerSha256 = '5472cceaf870f66e2a6580787fe8a12d3a1d6565e86585ee0ffb345c09385eae'
    CliSha256 = '0438ce9e17d469764cb194f19303fe7b5642d0c742a5de3a92e139ea4dd7fc35'
    GuardSha256 = '9343b0d7367926fd4365a4672ee51617b60d4e77f30e7f2642e481dbb7e8d2be'
    LayoutSha256 = '76e10389f88c377746112fd63f7959e09be1f36179aab9ebdb57b001a002b26a'
    QueueDatabase = 'F:\论文预处理系统\预处理系统2\local-intake-runtime\waiting-pool.sqlite3'
    Log = 'F:\论文预处理系统\预处理系统2\local-intake-runtime\postprocessing-observer-mainline.log'
    ReleaseManifest = 'F:\论文预处理系统\预处理系统2\candidates\postprocess-prefect-20261031\server_release_next\release-inventory-prefect-continuation-52fe4fef2691.json'
    ReleaseManifestSha256 = 'ab5e73b71ff579fb76999c3993d375723c242ceb1005631f12ef58ccee6cfde2'
    RemoteConfig = 'F:\论文预处理系统\预处理系统2\candidates\postprocess-retry-prefect-20261101\remote-config-next.json'
    RemoteConfigSha256 = 'f96ccf4e7e4fe1ee83bf1a0b6e3dfa9474968805a17a10c990b87ee9f3ad8e60'
    PartialObservation = $true
    HistoryRegistry = 'F:\论文预处理系统\预处理系统2\candidates\postprocess-retry-prefect-20261101\single-reader-registry.json'
    HistoryRegistrySha256 = '00515bb7b0816e3d96ee7216bce2d492593eab6b524fe67a1fa454491f0c3c28'
    HistoryAccess = (Join-Path $release 'history-access-mainline-20261101-v2.json')
    HistoryAccessSha256 = 'dee6caa06cf74b9ceb74866da925e8b049c9219ede17c8616db1f3bf736fa3d3'
    HistoryUnreadScript = '/home/wzy/paperflow-postprocess/candidate-20261101/retry-release-03ce92996fd8/server_unread_scope_query.py'
    HistoryUnreadScriptSha256 = '7ccce506412fb3096d96d1bc14572f4ff821ff9d36bacedac5d72927c96bb63b'
    GuardBundle = 'F:\论文预处理系统\预处理系统2\candidates\postprocess-retry-prefect-20261101\single-reader-bundle.json'
    GuardBundleSha256 = 'ecdaf88ea26436a3f80524416db7ef29b51e0f51b9d0226052b6af1cd0f7c472'
    Port = 14206
}
$bytes = [IO.File]::ReadAllBytes($launcherPath)
$hasher = [Security.Cryptography.SHA256]::Create()
try { $actual = -join ($hasher.ComputeHash($bytes) | ForEach-Object { $_.ToString('x2') }) }
finally { $hasher.Dispose() }
if ($actual -cne $expectedLauncherSha) { throw '观察启动器源码发生变化' }
& ([scriptblock]::Create([Text.Encoding]::UTF8.GetString($bytes))) @launchParameters
exit $LASTEXITCODE
