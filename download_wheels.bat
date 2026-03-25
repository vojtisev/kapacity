@echo off
setlocal EnableExtensions
title KAPACITY - stazeni wheelu pro offline instalaci
cd /d "%~dp0"

REM Volitelne: proxy pro pip
REM call "%~dp0set_proxy_mlp.bat"

echo Stahuji wheel soubory do slozky wheels\ ...
echo (vhodne na pocitaci s internetem; pak zkopirujte wheels\ k sobe a spustte build_portable.bat)
echo.

where py >nul 2>&1
if not errorlevel 1 (
  py -3 -m pip download -r "%~dp0requirements.txt" -d "%~dp0wheels"
  goto check
)
where python >nul 2>&1
if errorlevel 1 (
  echo [CHYBA] Potrebujete Python s pip ^(py -3 nebo python^).
  pause
  exit /b 1
)
python -m pip download -r "%~dp0requirements.txt" -d "%~dp0wheels"

:check
if errorlevel 1 (
  echo [CHYBA] pip download selhal.
  pause
  exit /b 1
)
echo.
echo Hotovo. Slozka: %~dp0wheels
pause
exit /b 0
