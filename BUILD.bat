@echo off
setlocal enabledelayedexpansion
title Stock Oracle — Build Installer
cd /d "%~dp0"
echo.
echo ========================================
echo  Stock Oracle — Build Standalone App
echo ========================================
echo.
echo This will create a standalone .exe that
echo anyone can run without installing Python.
echo.

REM ── Find Python ──
set PYTHON_CMD=
for %%V in (3.13 3.12 3.11) do (
    py -%%V --version >nul 2>&1
    if !ERRORLEVEL!==0 (
        set PYTHON_CMD=py -%%V
        goto :found
    )
)
python --version >nul 2>&1
if %ERRORLEVEL%==0 (
    set PYTHON_CMD=python
    goto :found
)
echo ERROR: Python not found!
pause
exit /b 1

:found
echo Using: %PYTHON_CMD%
%PYTHON_CMD% --version
echo.

REM ── Install build dependencies ──
echo [1/4] Installing build tools...
%PYTHON_CMD% -m pip install --quiet pyinstaller pillow

REM ── Install app dependencies ──
echo [2/4] Installing app dependencies...
%PYTHON_CMD% -m pip install --quiet -r stock_oracle\requirements.txt

REM ── Generate icon if not present ──
if not exist "stock_oracle\icon.ico" (
    echo [2.5/4] Generating app icon...
    %PYTHON_CMD% -c "from PIL import Image, ImageDraw; img=Image.new('RGBA',(256,256),(0,0,0,0)); d=ImageDraw.Draw(img); d.ellipse([20,20,236,236],fill='#1a1f2e',outline='#00cc66',width=12); d.text((80,85),'SO',fill='#00cc66'); img.save('stock_oracle/icon.ico',format='ICO',sizes=[(256,256),(128,128),(64,64),(48,48),(32,32),(16,16)])" 2>nul
)

REM ── Run PyInstaller ──
echo [3/4] Building standalone app (this takes 2-5 minutes)...
%PYTHON_CMD% -m PyInstaller ^
    --name "StockOracle" ^
    --windowed ^
    --noconfirm ^
    --clean ^
    --add-data "stock_oracle;stock_oracle" ^
    --hidden-import "stock_oracle" ^
    --hidden-import "stock_oracle.gui" ^
    --hidden-import "stock_oracle.oracle" ^
    --hidden-import "stock_oracle.config" ^
    --hidden-import "stock_oracle.ml" ^
    --hidden-import "stock_oracle.ml.pipeline" ^
    --hidden-import "stock_oracle.collectors" ^
    --hidden-import "stock_oracle.collectors.base" ^
    --hidden-import "stock_oracle.collectors.yahoo_finance" ^
    --hidden-import "stock_oracle.collectors.finnhub_collector" ^
    --hidden-import "stock_oracle.collectors.analysis" ^
    --hidden-import "stock_oracle.collectors.advanced_signals" ^
    --hidden-import "stock_oracle.collectors.alt_data" ^
    --hidden-import "stock_oracle.collectors.creative_signals" ^
    --hidden-import "stock_oracle.collectors.cross_stock" ^
    --hidden-import "stock_oracle.collectors.reddit_sentiment" ^
    --hidden-import "stock_oracle.collectors.sec_edgar" ^
    --hidden-import "stock_oracle.collectors.job_postings" ^
    --hidden-import "stock_oracle.collectors.new_indicators" ^
    --hidden-import "stock_oracle.collectors.viral_catalyst" ^
    --hidden-import "stock_oracle.collectors.realtime_news" ^
    --hidden-import "stock_oracle.session_tracker" ^
    --hidden-import "stock_oracle.prediction_tracker" ^
    --hidden-import "stock_oracle.signal_intelligence" ^
    --hidden-import "stock_oracle.breakout_detector" ^
    --hidden-import "stock_oracle.market_regime" ^
    --hidden-import "stock_oracle.news_feed" ^
    --hidden-import "stock_oracle.claude_advisor" ^
    --hidden-import "stock_oracle.narrative" ^
    --hidden-import "stock_oracle.trainer" ^
    --hidden-import "stock_oracle.historical_trainer" ^
    --hidden-import "stock_oracle.ollama_nlp" ^
    --hidden-import "stock_oracle.setup_wizard" ^
    --hidden-import "stock_oracle.utils" ^
    --hidden-import "sklearn" ^
    --hidden-import "sklearn.ensemble" ^
    --hidden-import "sklearn.preprocessing" ^
    --hidden-import "sklearn.model_selection" ^
    --hidden-import "numpy" ^
    --hidden-import "yfinance" ^
    --hidden-import "anthropic" ^
    --hidden-import "flask" ^
    --hidden-import "PIL" ^
    --hidden-import "pystray" ^
    --hidden-import "zoneinfo" ^
    --exclude-module "matplotlib" ^
    --exclude-module "IPython" ^
    --exclude-module "jupyter" ^
    --exclude-module "notebook" ^
    --exclude-module "pytest" ^
    --exclude-module "sphinx" ^
    stock_oracle\launcher.py

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo BUILD FAILED! Check errors above.
    pause
    exit /b 1
)

REM ── Post-build ──
echo [4/4] Finalizing...

REM Copy icon if it exists
if exist "stock_oracle\icon.ico" (
    copy /Y "stock_oracle\icon.ico" "dist\StockOracle\" >nul 2>&1
)

REM Create a README for the distribution
(
echo Stock Oracle
echo ============
echo.
echo Double-click StockOracle.exe to launch.
echo.
echo First Run:
echo   1. Click Settings (gear icon^) to add API keys
echo   2. Finnhub: free at finnhub.io (real-time prices^)
echo   3. Anthropic: console.anthropic.com (AI advisor, optional^)
echo   4. Click "Start Monitoring" to begin
echo.
echo Your data is stored in: %%APPDATA%%\StockOracle\
echo This means you can update the app without losing your
echo predictions, sessions, ML models, or settings.
echo.
echo Built with Stock Oracle by James Cupps
) > "dist\StockOracle\README.txt"

echo.
echo ========================================
echo  BUILD COMPLETE!
echo ========================================
echo.
echo Output: dist\StockOracle\
echo   - StockOracle.exe (main app)
echo   - README.txt
echo.
echo To distribute:
echo   1. Zip the dist\StockOracle folder
echo   2. Send the zip to anyone
echo   3. They unzip and double-click StockOracle.exe
echo.
echo To create a Windows installer (optional):
echo   - Install Inno Setup: https://jrsoftware.org/isinfo.php
echo   - Run: iscc installer.iss
echo.
pause
