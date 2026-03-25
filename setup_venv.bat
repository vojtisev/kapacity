@echo off
setlocal EnableExtensions
title KAPACITY - setup venv
cd /d "%~dp0"
call "%~dp0set_proxy_mlp.bat"

echo.
echo ========================================
echo   KAPACITY - virtualni prostredi
echo ========================================
echo Slozka: %CD%
echo.

set "PY_CMD="
where py >nul 2>&1
if not errorlevel 1 (
  py -3 --version >nul 2>&1
  if not errorlevel 1 (
    set "PY_CMD=py -3"
    echo Nalezen: py -3
    goto have_python
  )
)
where python >nul 2>&1
if errorlevel 1 (
  echo [CHYBA] Nenalezen Python ani prikaz py.
  echo Nainstalujte Python 3 z https://www.python.org/downloads/
  echo Pri instalaci zaskrtnete "Add python.exe to PATH".
  goto finish_fail
)
set "PY_CMD=python"
echo Nalezen: python

:have_python
echo.
echo Vytvarim .venv ...
%PY_CMD% -m venv .venv
if errorlevel 1 (
  echo [CHYBA] python -m venv selhal.
  echo Tip: zkuste projekt zkopirovat na lokalni disk ^(C:\...^) misto UNC.
  goto finish_fail
)
if not exist ".venv\Scripts\python.exe" (
  echo [CHYBA] Chybi .venv\Scripts\python.exe
  goto finish_fail
)

set "VENV_PY=%~dp0.venv\Scripts\python.exe"

REM Delsi timeout — firemni site casto blokuji nebo zpomaluji pypi.org
set "PIP_OPTS=--default-timeout=120 --retries 10"

REM Vzdy pip z tohoto venv ^(activate.bat nekdy neprepise PATH — napr. OneDrive^)
echo.
echo Aktualizuji pip ^(pypi.org musi byt dostupny — jinak viz docs/PIP-FIREWALL.md^) ...
"%VENV_PY%" -m pip install %PIP_OPTS% --upgrade pip
if errorlevel 1 (
  echo [VAROVANI] Aktualizace pip selhala — zkousim pokracovat se stavajicim pip...
)

echo.
echo Instaluji zavislosti z requirements.txt ...
"%VENV_PY%" -m pip install %PIP_OPTS% -r "%~dp0requirements.txt"
if errorlevel 1 (
  echo [CHYBA] pip install selhal — typicky firewall/proxy na praci.
  echo Viz docs/PIP-FIREWALL.md ^(proxy, jina sit, stazeni baliku jinde^).
  goto finish_fail
)

echo.
echo Kontroluji instalaci ^(streamlit, pandas^) ...
"%VENV_PY%" -c "import streamlit, pandas; print('OK: baliky v .venv')"
if errorlevel 1 (
  echo [CHYBA] Baliky nejdou importovat — zkuste smazat slozku .venv a spustit znovu.
  goto finish_fail
)

echo.
echo ========================================
echo   Hotovo. Spustte run_dashboard.bat
echo   (koncovi uzivatele bez instalace Pythonu: viz build_portable.bat)
echo ========================================
goto finish_ok

:finish_fail
echo.
echo ========================================
echo   Skoncilo chybou
echo ========================================
echo.
echo Stisknete klavesu pro zavreni okna...
pause
exit /b 1

:finish_ok
echo.
echo Stisknete klavesu pro zavreni okna...
pause
exit /b 0
