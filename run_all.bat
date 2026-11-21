@echo off
REM Force UTF-8 console encoding so Unicode pipeline output cannot crash cp1252.
set PYTHONIOENCODING=utf-8
REM ============================================================================
REM One-Click Master Pipeline Launcher (Windows)
REM News Article Analysis and Topic Extraction (Big Data Architecture)
REM ============================================================================
title News Article Big Data Pipeline Launcher
echo ============================================================================
echo   STARTING BIG DATA PIPELINE ORCHESTRATOR
echo   Hadoop Ecosystem (HDFS, YARN, MapReduce) ^| Spark ^| MongoDB ^| Streamlit
echo ============================================================================
echo.

python --version >nul 2>&1
if errorlevel 1 (
    echo [!] Python is not found on PATH. Please install Python 3.10+ and add to PATH.
    pause
    exit /b 1
)

python run_all.py

echo.
echo ============================================================================
echo   PIPELINE FINISHED! Launching Streamlit Interactive Web Dashboard...
echo ============================================================================
echo.
streamlit run src/dashboard.py

pause
