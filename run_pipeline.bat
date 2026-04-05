@echo off
setlocal enabledelayedexpansion

echo ========================================================
echo Starting Real Estate Scraper Pipeline (Phases 1-9)
echo ========================================================
echo.

:: 1. Environment Setup & Backup
echo [1/3] Setting up environment and backing up old outputs...
if exist ".venv\Scripts\activate.bat" (
    echo Activating Virtual Environment...
    call .venv\Scripts\activate.bat
) else (
    echo [WARNING] .venv\Scripts\activate.bat not found. Continuing with global Python...
)

:: Safely get current timestamp string YYYYMMDD_HHMMSS
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set datetime=%%I
set TIMESTAMP=%datetime:~0,4%%datetime:~4,2%%datetime:~6,2%_%datetime:~8,2%%datetime:~10,2%%datetime:~12,2%
set BACKUP_DIR=backups\output_backup_%TIMESTAMP%

if exist "output\" (
    echo Creating backup at %BACKUP_DIR%...
    mkdir "%BACKUP_DIR%"
    :: We ignore xcopy error if the folder is empty
    xcopy /E /I /Y "output\*" "%BACKUP_DIR%\" >nul 2>&1
    
    echo Backup successful. Clearing current output directory...
    del /Q /S "output\*" >nul 2>&1
) else (
    echo Creating output directory...
    mkdir "output"
)

:: 2. Phase Execution
echo.
echo ========================================================
echo [2/3] Executing pipeline phases...
echo ========================================================

echo.
echo --- Phase 1: Scraping URLs ---
python scraper_icasa.py --urls-only
if %errorlevel% neq 0 (
    echo [ERROR] Phase 1 failed!
    pause
    exit /b %errorlevel%
)

echo.
echo --- Phase 2: Checking URLs against API ---
:: Using 10 workers for faster fetching to the API
python check_urls.py --buy output/buy_urls.txt --rent output/rent_urls.txt --out output/results.jsonl --json output/result.json --workers 10 --delay 0.0
if %errorlevel% neq 0 (
    echo [ERROR] Phase 2 failed!
    pause
    exit /b %errorlevel%
)

echo.
echo --- Phase 3: Scraping Missing Detail Pages ---
:: Using 15 workers for significantly faster HTML downloading
python phase3_scrape.py --workers 15 --delay 0.0
if %errorlevel% neq 0 (
    echo [ERROR] Phase 3 failed!
    pause
    exit /b %errorlevel%
)

echo.
echo --- Phase 4: Cleaning Agencies ---
python phase4_clean.py
if %errorlevel% neq 0 (
    echo [ERROR] Phase 4 failed!
    pause
    exit /b %errorlevel%
)

echo.
echo --- Phase 5: Executing API Contact Checks ---
python phase5_api.py
if %errorlevel% neq 0 (
    echo [ERROR] Phase 5 failed!
    pause
    exit /b %errorlevel%
)

echo.
echo --- Phase 8: Applying External IDs and Categories ---
python phase8_process.py
if %errorlevel% neq 0 (
    echo [ERROR] Phase 8 failed!
    pause
    exit /b %errorlevel%
)

echo.
echo ========================================================
echo [3/3] Pipeline completed successfully!
echo Final results are in the output/ directory.
echo ========================================================
echo.
pause
