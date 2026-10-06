@echo off
title Stock Oracle Terminal
cd /d "%~dp0"

REM Usage:  TERMINAL.bat            this PC only (opens your browser)
REM         TERMINAL.bat --lan      also reachable from your phone/laptop over Tailscale or LAN
REM                                 (prints a URL with an access token; open it once per device)

echo Starting Stock Oracle Terminal...

py -3.13 -c "import fastapi, uvicorn, yfinance" >nul 2>&1
if %ERRORLEVEL%==0 (
    py -3.13 -m stock_oracle.terminal %*
    goto :end
)

py -3.12 -c "import fastapi, uvicorn, yfinance" >nul 2>&1
if %ERRORLEVEL%==0 (
    py -3.12 -m stock_oracle.terminal %*
    goto :end
)

py -3.11 -c "import fastapi, uvicorn, yfinance" >nul 2>&1
if %ERRORLEVEL%==0 (
    py -3.11 -m stock_oracle.terminal %*
    goto :end
)

python -c "import fastapi, uvicorn, yfinance" >nul 2>&1
if %ERRORLEVEL%==0 (
    python -m stock_oracle.terminal %*
    goto :end
)

echo.
echo Terminal packages not found. Installing dependencies...
echo.

py -3.13 --version >nul 2>&1
if %ERRORLEVEL%==0 (
    py -3.13 -m pip install -r stock_oracle\requirements.txt
    py -3.13 -m stock_oracle.terminal %*
    goto :end
)

python -m pip install -r stock_oracle\requirements.txt
python -m stock_oracle.terminal %*

:end
pause
