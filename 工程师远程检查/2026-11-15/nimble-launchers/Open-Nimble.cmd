@echo off
chcp 65001 >nul
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0Start-Nimble.ps1" -Quiet
if errorlevel 1 (
  pause
  exit /b 1
)
call "%~dp0ollama.cmd" run nimble
if errorlevel 1 pause
