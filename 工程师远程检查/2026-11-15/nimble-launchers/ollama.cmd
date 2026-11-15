@echo off
setlocal
set "OLLAMA_HOST=127.0.0.1:11434"
set "OLLAMA_MODELS=%~dp0models"
"%~dp0runtime\ollama.exe" %*
exit /b %errorlevel%
