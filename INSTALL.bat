@echo off
setlocal enabledelayedexpansion
title Stock Oracle — Installer
cd /d "%~dp0"
color 0A

echo.
echo  ╔══════════════════════════════════════════════════╗
echo  ║         Stock Oracle — One-Click Install         ║
echo  ║                                                  ║
echo  ║  Stock prediction system with 38 data signals,   ║
echo  ║  machine learning, breakout scanner, and          ║
echo  ║  optional Claude AI advisor.                      ║
echo  ╚══════════════════════════════════════════════════╝
echo.

REM ═══════════════════════════════════════════════════════
REM  Step 1: Check for Python
REM ═══════════════════════════════════════════════════════
echo  [Step 1/4] Checking for Python...
echo.

set PYTHON_CMD=

REM Try py launcher with specific versions
for %%V in (3.13 3.12 3.11) do (
    py -%%V --version >nul 2>&1
    if !ERRORLEVEL!==0 (
        set PYTHON_CMD=py -%%V
        goto :python_found
    )
)

REM Try generic python
python --version >nul 2>&1
if %ERRORLEVEL%==0 (
    set PYTHON_CMD=python
    goto :python_found
)

REM Try python3
python3 --version >nul 2>&1
if %ERRORLEVEL%==0 (
    set PYTHON_CMD=python3
    goto :python_found
)

REM No Python found — offer to download
echo  ╔══════════════════════════════════════════════════╗
echo  ║  Python is not installed.                        ║
echo  ║                                                  ║
echo  ║  Stock Oracle needs Python to run.               ║
echo  ║  Opening the download page now...                ║
echo  ║                                                  ║
echo  ║  IMPORTANT: During install, check the box:       ║
echo  ║  [x] "Add Python to PATH"                       ║
echo  ║                                                  ║
echo  ║  After Python installs, run this installer       ║
echo  ║  again (double-click INSTALL.bat).               ║
echo  ╚══════════════════════════════════════════════════╝
echo.
start https://www.python.org/downloads/
echo  Press any key after you've installed Python...
pause >nul
echo.

REM Retry after install
for %%V in (3.13 3.12 3.11) do (
    py -%%V --version >nul 2>&1
    if !ERRORLEVEL!==0 (
        set PYTHON_CMD=py -%%V
        goto :python_found
    )
)
python --version >nul 2>&1
if %ERRORLEVEL%==0 (
    set PYTHON_CMD=python
    goto :python_found
)

echo  Still can't find Python. Please restart your
echo  computer and try again after installing Python.
pause
exit /b 1

:python_found
echo  Found: %PYTHON_CMD%
for /f "delims=" %%i in ('%PYTHON_CMD% --version 2^>^&1') do echo  Version: %%i
echo.

REM ═══════════════════════════════════════════════════════
REM  Step 2: Install dependencies
REM ═══════════════════════════════════════════════════════
echo  [Step 2/4] Installing dependencies (this may take a minute)...
echo.

%PYTHON_CMD% -m pip install --upgrade pip --quiet 2>nul
%PYTHON_CMD% -m pip install -r stock_oracle\requirements.txt --quiet 2>nul

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo  Some packages had issues. Trying individually...
    %PYTHON_CMD% -m pip install requests numpy yfinance scikit-learn flask --quiet 2>nul
    %PYTHON_CMD% -m pip install anthropic pystray Pillow --quiet 2>nul
)

echo  Dependencies installed.
echo.

REM ═══════════════════════════════════════════════════════
REM  Step 3: Create shortcuts
REM ═══════════════════════════════════════════════════════
echo  [Step 3/4] Creating shortcuts...
echo.

set APP_DIR=%~dp0
set DESKTOP=%USERPROFILE%\Desktop
set STARTMENU=%APPDATA%\Microsoft\Windows\Start Menu\Programs

REM Create a .bat launcher that the shortcuts point to
(
echo @echo off
echo cd /d "%APP_DIR%"
echo start "" /min cmd /c "%PYTHON_CMD% -m stock_oracle"
) > "%APP_DIR%StockOracle.bat"

REM Create desktop shortcut via PowerShell
powershell -NoProfile -Command ^
    "$ws = New-Object -ComObject WScript.Shell; ^
     $s = $ws.CreateShortcut('%DESKTOP%\Stock Oracle.lnk'); ^
     $s.TargetPath = '%APP_DIR%StockOracle.bat'; ^
     $s.WorkingDirectory = '%APP_DIR%'; ^
     $s.WindowStyle = 7; ^
     $s.Description = 'Stock Oracle — Stock Prediction System'; ^
     $s.Save()" 2>nul

if %ERRORLEVEL%==0 (
    echo  Desktop shortcut created.
) else (
    echo  Could not create desktop shortcut (not critical^).
)

REM Create Start Menu shortcut
powershell -NoProfile -Command ^
    "$ws = New-Object -ComObject WScript.Shell; ^
     $s = $ws.CreateShortcut('%STARTMENU%\Stock Oracle.lnk'); ^
     $s.TargetPath = '%APP_DIR%StockOracle.bat'; ^
     $s.WorkingDirectory = '%APP_DIR%'; ^
     $s.WindowStyle = 7; ^
     $s.Description = 'Stock Oracle — Stock Prediction System'; ^
     $s.Save()" 2>nul

if %ERRORLEVEL%==0 (
    echo  Start Menu shortcut created.
)
echo.

REM ═══════════════════════════════════════════════════════
REM  Step 4: Done!
REM ═══════════════════════════════════════════════════════
echo  ╔══════════════════════════════════════════════════╗
echo  ║                                                  ║
echo  ║         Installation Complete!                    ║
echo  ║                                                  ║
echo  ║  You can now:                                    ║
echo  ║  • Double-click "Stock Oracle" on your Desktop   ║
echo  ║  • Or find it in your Start Menu                 ║
echo  ║                                                  ║
echo  ║  First Launch Tips:                              ║
echo  ║  • A setup wizard will guide you through config  ║
echo  ║  • Get a FREE API key at finnhub.io for live     ║
echo  ║    prices (optional but recommended)             ║
echo  ║  • Click "Help" in the app for a full guide      ║
echo  ║                                                  ║
echo  ╚══════════════════════════════════════════════════╝
echo.

set /p LAUNCH="  Launch Stock Oracle now? (Y/n): "
if /i "!LAUNCH!"=="n" goto :end

echo.
echo  Starting Stock Oracle...
cd /d "%APP_DIR%"
%PYTHON_CMD% -m stock_oracle

:end
echo.
echo  You can always launch from your Desktop shortcut.
echo  Press any key to close this window.
pause >nul
