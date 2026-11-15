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
    ReleaseManifest = (Join-Path $release 'release-inventory-three-card-v2-20261101.json')
    ReleaseManifestSha256 = 'e0e9d82bad62708d4fd0ef06c64be126d5439ad28d63623f8889f078687499ed'
    RemoteConfig = 'F:\论文预处理系统\预处理系统2\candidates\postprocess-server-20261101\optimization-20261101\remote-config.json'
    RemoteConfigSha256 = 'f454987e9f13b806c8fe28c20afac58a80112142acaa8ea4d8c6d72197929a69'
    PartialObservation = $true
    HistoryRegistry = 'F:\论文预处理系统\预处理系统2\candidates\postprocess-server-20261101\optimization-20261101\history-registry.json'
    HistoryRegistrySha256 = '1d025c77444e8fc20dbb37532dd00a7a843163733d214afafcbd8140cbd72fe0'
    HistoryAccess = (Join-Path $release 'history-access-mainline-20261101-v2.json')
    HistoryAccessSha256 = 'dee6caa06cf74b9ceb74866da925e8b049c9219ede17c8616db1f3bf736fa3d3'
    HistoryUnreadScript = '/home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v3/server_unread_scope_query.py'
    HistoryUnreadScriptSha256 = 'f8f00ff11b82836b107fd11031c40249a8644b84789d397f80e5827dfe137521'
    GuardBundle = 'F:\论文预处理系统\预处理系统2\candidates\postprocess-server-20261101\optimization-20261101\guard-bundle.json'
    GuardBundleSha256 = '55343ae0a384177a570b594a478855791b0da6838c1964e0c86a102feb7d2cca'
    Port = 14206
}
$bytes = [IO.File]::ReadAllBytes($launcherPath)
$hasher = [Security.Cryptography.SHA256]::Create()
try { $actual = -join ($hasher.ComputeHash($bytes) | ForEach-Object { $_.ToString('x2') }) }
finally { $hasher.Dispose() }
if ($actual -cne $expectedLauncherSha) { throw '观察启动器源码发生变化' }
& ([scriptblock]::Create([Text.Encoding]::UTF8.GetString($bytes))) @launchParameters
exit $LASTEXITCODE
