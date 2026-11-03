@echo off
rem ============================================================
rem  Super Agent - terminal console
rem  Keep this file ASCII-only. Chinese text inside a .bat breaks
rem  cmd parsing on some Windows codepages.
rem ============================================================
setlocal
cd /d "%~dp0"
title Super Agent Console

set "PY="
python -c "import sys" >nul 2>nul
if not errorlevel 1 set "PY=python"
if defined PY goto check_deps

py -3 -c "import sys" >nul 2>nul
if not errorlevel 1 set "PY=py -3"
if defined PY goto check_deps

echo [ERROR] Python not found. Install Python 3.10+ and check "Add to PATH".
echo         https://www.python.org/downloads/
echo.
pause
exit /b 1

:check_deps
%PY% -c "import agentscope, textual, rich" >nul 2>nul
if not errorlevel 1 goto run
echo Installing dependencies for the first run...
%PY% -m pip install -r requirements.txt
%PY% -c "import agentscope, textual, rich" >nul 2>nul
if not errorlevel 1 goto run
echo.
echo [ERROR] Dependencies are still missing. Run this manually to see why:
echo         %PY% -m pip install -r requirements.txt
echo.
pause
exit /b 1

:run
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
%PY% main.py %*
set "CODE=%ERRORLEVEL%"
echo.
echo Exited with code %CODE%
echo If that was not intentional, keep the newest folder under logs\
echo and send it for diagnosis.
echo.
pause
endlocal
exit /b %CODE%
