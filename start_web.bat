@echo off
rem ============================================================
rem  Super Agent - browser console
rem  ASCII only. The browser is opened by web.py itself, after the
rem  server has bound its port; opening it here would race.
rem ============================================================
setlocal
cd /d "%~dp0"
title Super Agent Console (Web)

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
%PY% web.py --port 8080 %*
set "CODE=%ERRORLEVEL%"
echo.
echo Exited with code %CODE%
echo If the page did not open, visit http://127.0.0.1:8080 manually.
echo Keep the newest folder under logs\ if you need it diagnosed.
echo.
pause
endlocal
exit /b %CODE%
