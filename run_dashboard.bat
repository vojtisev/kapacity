@echo off
setlocal EnableExtensions
title KAPACITY - dashboard
cd /d "%~dp0"
call "%~dp0set_proxy_mlp.bat"

echo.
echo ========================================
echo   KAPACITY - Streamlit dashboard
echo ========================================
echo Slozka: %CD%
echo.

set "PY_CMD="
if exist "%~dp0runtime\python\python.exe" (
  set "PY_CMD=%~dp0runtime\python\python.exe"
  echo Pouzivam prenosny balicek: runtime\python
  goto have_python
)
if exist "%~dp0.venv\Scripts\python.exe" (
  set "PY_CMD=%~dp0.venv\Scripts\python.exe"
  echo Pouzivam virtualni prostredi: .venv
  goto have_python
)

echo [CHYBA] Nenalezen Python pro spusteni.
echo.
echo  Varianta A (offline): nejdriv jednou spustte build_portable.bat
echo        ^(vytvori runtime\python\ - viz docs\PORTABLE-OFFLINE-BALICEK.md^).
echo.
echo  Varianta B: spustte setup_venv.bat ^(potreba Python v PATH a pip z internetu^).
echo.
pause
exit /b 1

:have_python
echo Spoustim Streamlit ^(Ctrl+C ukonci server^) ...
echo.
"%PY_CMD%" -m streamlit run "%~dp0app.py"
echo.
echo Streamlit skoncil.
echo.
pause
