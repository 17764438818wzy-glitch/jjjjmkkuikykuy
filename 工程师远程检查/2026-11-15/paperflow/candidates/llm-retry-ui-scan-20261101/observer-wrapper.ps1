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
    ReleaseManifest = (Join-Path $release 'release-inventory-retry-queue-20261101.json')
    ReleaseManifestSha256 = '94b2ba7d732f4e2819bb2eeb0b415bab6b57c31d6feb005f5af4fade31dc0f43'
    RemoteConfig = 'F:\论文预处理系统\预处理系统2\prefect-paperflow\runtime\remote-postprocessing\mainline-remote-config-21b7b881c8354ecf88dd2b316abfee12.json'
    RemoteConfigSha256 = 'eb7a49f56daeeef3b2f0fd0bc92d12200176fbfff3650799d3b5fcb55c4ae42b'
    PartialObservation = $true
    HistoryRegistry = (Join-Path $release 'history-registry-mainline-20261101-v2.json')
    HistoryRegistrySha256 = '171284e05255067b6bf501060b4bd6bbe94894d10021e0c5e0370ea93edffb12'
    HistoryAccess = (Join-Path $release 'history-access-mainline-20261101-v2.json')
    HistoryAccessSha256 = 'dee6caa06cf74b9ceb74866da925e8b049c9219ede17c8616db1f3bf736fa3d3'
    HistoryUnreadScript = '/home/wzy/paperflow-postprocess/candidate-20261101/readonly-page-query-20261101-v3/server_unread_scope_query.py'
    HistoryUnreadScriptSha256 = 'f8f00ff11b82836b107fd11031c40249a8644b84789d397f80e5827dfe137521'
    GuardBundle = (Join-Path $release 'guard-bundle-mainline-20261101.json')
    GuardBundleSha256 = '2745dc7f33e4e609e8cb7b5622d65cbacf286328a13e6ea0de02d9ede2fc1795'
    Port = 14206
}
$bytes = [IO.File]::ReadAllBytes($launcherPath)
$hasher = [Security.Cryptography.SHA256]::Create()
try { $actual = -join ($hasher.ComputeHash($bytes) | ForEach-Object { $_.ToString('x2') }) }
finally { $hasher.Dispose() }
if ($actual -cne $expectedLauncherSha) { throw '观察启动器源码发生变化' }
& ([scriptblock]::Create([Text.Encoding]::UTF8.GetString($bytes))) @launchParameters
exit $LASTEXITCODE
