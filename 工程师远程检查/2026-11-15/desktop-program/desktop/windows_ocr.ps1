param([Parameter(Mandatory=$true)][string]$ImagePath)
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)
Add-Type -AssemblyName System.Runtime.WindowsRuntime
$null = [Windows.Storage.StorageFile, Windows.Storage, ContentType=WindowsRuntime]
$null = [Windows.Graphics.Imaging.BitmapDecoder, Windows.Foundation, ContentType=WindowsRuntime]
$null = [Windows.Media.Ocr.OcrEngine, Windows.Foundation, ContentType=WindowsRuntime]
$null = [Windows.Globalization.Language, Windows.Globalization, ContentType=WindowsRuntime]
$taskAwaitMethod = [System.WindowsRuntimeSystemExtensions].GetMethods() | Where-Object {
    $_.Name -eq 'AsTask' -and $_.IsGenericMethod -and $_.GetParameters().Count -eq 1 -and $_.GetParameters()[0].ParameterType.Name -eq 'IAsyncOperation`1'
} | Select-Object -First 1
function Wait-OcrOperation($Operation, [Type]$ResultType) {
    $taskAsync = $taskAwaitMethod.MakeGenericMethod($ResultType).Invoke($null, @($Operation))
    $taskAsync.Wait()
    return $taskAsync.Result
}
$taskFile = Wait-OcrOperation ([Windows.Storage.StorageFile]::GetFileFromPathAsync($ImagePath)) ([Windows.Storage.StorageFile])
$taskStream = Wait-OcrOperation ($taskFile.OpenReadAsync()) ([Windows.Storage.Streams.IRandomAccessStreamWithContentType])
try {
    $taskDecoder = Wait-OcrOperation ([Windows.Graphics.Imaging.BitmapDecoder]::CreateAsync($taskStream)) ([Windows.Graphics.Imaging.BitmapDecoder])
    $taskBitmap = Wait-OcrOperation ($taskDecoder.GetSoftwareBitmapAsync()) ([Windows.Graphics.Imaging.SoftwareBitmap])
    $taskLanguage = New-Object Windows.Globalization.Language('zh-Hans-CN')
    $taskEngine = [Windows.Media.Ocr.OcrEngine]::TryCreateFromLanguage($taskLanguage)
    if ($null -eq $taskEngine) { throw 'Chinese OCR language is unavailable' }
    $taskResult = Wait-OcrOperation ($taskEngine.RecognizeAsync($taskBitmap)) ([Windows.Media.Ocr.OcrResult])
    $taskLines = @($taskResult.Lines | ForEach-Object {
        @{ text=$_.Text; words=@($_.Words | ForEach-Object { @{ text=$_.Text; x=$_.BoundingRect.X; y=$_.BoundingRect.Y; width=$_.BoundingRect.Width; height=$_.BoundingRect.Height } }) }
    })
    @{ engine='Windows.Media.Ocr'; language='zh-Hans-CN'; text=$taskResult.Text; lines=$taskLines } | ConvertTo-Json -Depth 8 -Compress
} finally {
    if ($taskBitmap) { $taskBitmap.Dispose() }
    $taskStream.Dispose()
}
