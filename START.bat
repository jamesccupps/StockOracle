@echo off
title Stock Oracle
cd /d "%~dp0"

REM ── Find the right Python with yfinance installed ──
echo Starting Stock Oracle...

REM Try Python 3.13 first (known to work with yfinance)
py -3.13 -c "import yfinance" >nul 2>&1
if %ERRORLEVEL%==0 (
    echo Using Python 3.13
    py -3.13 -m stock_oracle
    goto :end
)

REM Try Python 3.12
py -3.12 -c "import yfinance" >nul 2>&1
if %ERRORLEVEL%==0 (
    echo Using Python 3.12
    py -3.12 -m stock_oracle
    goto :end
)

REM Try Python 3.11
py -3.11 -c "import yfinance" >nul 2>&1
if %ERRORLEVEL%==0 (
    echo Using Python 3.11
    py -3.11 -m stock_oracle
    goto :end
)

REM Try default python
python -c "import yfinance" >nul 2>&1
if %ERRORLEVEL%==0 (
    echo Using default Python
    python -m stock_oracle
    goto :end
)

REM Nothing works — install dependencies
echo.
echo yfinance not found on any Python version.
echo Attempting to install dependencies...
echo.

REM Try 3.13 first
py -3.13 --version >nul 2>&1
if %ERRORLEVEL%==0 (
    echo Installing on Python 3.13...
    py -3.13 -m pip install -r stock_oracle\requirements.txt
    py -3.13 -m stock_oracle
    goto :end
)

REM Fallback to default
echo Installing on default Python...
python -m pip install -r stock_oracle\requirements.txt
python -m stock_oracle

:end
pause
