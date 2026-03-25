@echo off
setlocal EnableExtensions
title KAPACITY - sestaveni prenosneho balicku (offline runtime)
cd /d "%~dp0"

REM Volitelne: firemni proxy pro pip (jen pri sestavovani)
REM call "%~dp0set_proxy_mlp.bat"

echo.
echo ========================================
echo   KAPACITY - sestaveni runtime\python
echo   (embeddable Python + zavislosti)
echo ========================================
echo Tento krok se provadi JEDNOU na pocitaci s Pythonem
echo NEBO s PowerShell a pristupem k internetu (PyPI).
echo Vysledek: slozka runtime\ - lze zkopirovat s celym projektem.
echo.
echo Podrobny popis: docs\PORTABLE-OFFLINE-BALICEK.md
echo.

where powershell >nul 2>&1
if errorlevel 1 (
  echo [CHYBA] Nenalezen PowerShell.
  pause
  exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\build_portable.ps1" %*
if errorlevel 1 (
  echo.
  echo [CHYBA] Sestaveni selhalo.
  pause
  exit /b 1
)

echo.
pause
exit /b 0
