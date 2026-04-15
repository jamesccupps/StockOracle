@echo off
title Stock Oracle - Setup
cd /d "%~dp0"
echo.
echo ========================================
echo  Stock Oracle - First Time Setup
echo ========================================
echo.

REM ── Find best Python version ──
set PYTHON_CMD=

REM Try 3.13 (best compatibility)
py -3.13 --version >nul 2>&1
if %ERRORLEVEL%==0 (
    set PYTHON_CMD=py -3.13
    echo Found Python 3.13 - recommended
    goto :install
)

REM Try 3.12
py -3.12 --version >nul 2>&1
if %ERRORLEVEL%==0 (
    set PYTHON_CMD=py -3.12
    echo Found Python 3.12
    goto :install
)

REM Try 3.11
py -3.11 --version >nul 2>&1
if %ERRORLEVEL%==0 (
    set PYTHON_CMD=py -3.11
    echo Found Python 3.11
    goto :install
)

REM Try default
python --version >nul 2>&1
if %ERRORLEVEL%==0 (
    set PYTHON_CMD=python
    echo Found default Python
    goto :install
)

echo ERROR: Python not found!
echo Download from https://www.python.org/downloads/
pause
exit /b 1

:install
echo.
echo Using: %PYTHON_CMD%
%PYTHON_CMD% --version
echo.
echo Installing dependencies...
echo.
%PYTHON_CMD% -m pip install --upgrade pip
%PYTHON_CMD% -m pip install -r stock_oracle\requirements.txt
echo.
echo ========================================
echo  Setup complete!
echo  Double-click START.bat to launch
echo ========================================
echo.
pause
