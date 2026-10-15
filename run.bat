@echo off
setlocal enabledelayedexpansion

:: 1. Always change current directory to the folder containing this batch script
cd /d "%~dp0"

echo ====================================================================
echo        Starting Fake News Classification Pipeline
echo        Kafka ^| Hadoop HDFS ^| TF-IDF NLP ^| Docker
echo ====================================================================

:: 2. Check if Docker is installed
where docker >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Docker is not installed or not in PATH. Please install Docker Desktop.
    pause
    exit /b 1
)

:: 3. Locate the Compose configuration file
set "COMPOSE_FILE=%~dp0docker-compose.yml"
if not exist "!COMPOSE_FILE!" (
    set "COMPOSE_FILE=%~dp0compose.yaml"
)

if not exist "!COMPOSE_FILE!" (
    echo [ERROR] Could not find docker-compose.yml or compose.yaml in "%~dp0"!
    echo Please ensure you are running run.bat from inside the cloned project directory.
    pause
    exit /b 1
)

echo [1/3] Found configuration file: !COMPOSE_FILE!

:: 4. Ensure runtime host directories exist for volume mounts
if not exist "%~dp0data\models" mkdir "%~dp0data\models"
if not exist "%~dp0data\raw" mkdir "%~dp0data\raw"
if not exist "%~dp0data\hdfs_buffer" mkdir "%~dp0data\hdfs_buffer"

:: 5. Determine Docker Compose command
docker compose version >nul 2>nul
if %ERRORLEVEL% equ 0 (
    set "COMPOSE_CMD=docker compose -f "!COMPOSE_FILE!""
) else (
    docker-compose version >nul 2>nul
    if %ERRORLEVEL% equ 0 (
        set "COMPOSE_CMD=docker-compose -f "!COMPOSE_FILE!""
    ) else (
        echo [ERROR] Neither 'docker compose' nor 'docker-compose' was found.
        pause
        exit /b 1
    )
)

echo.
echo [2/3] Building and starting all pipeline services...
!COMPOSE_CMD! up --build -d

if %ERRORLEVEL% neq 0 (
    echo.
    echo [ERROR] Docker Compose failed to start containers.
    echo Please ensure Docker Desktop is running.
    pause
    exit /b %ERRORLEVEL%
)

echo.
echo ====================================================================
echo [3/3] All Services Started Successfully!
echo ====================================================================
echo  - Interactive Web Dashboard : http://localhost:8000
echo  - Interactive Swagger API   : http://localhost:8000/docs
echo  - Hadoop HDFS Web Console   : http://localhost:9870
echo  - Kafka Host Broker         : localhost:29092
echo ====================================================================
echo.
echo To view real-time logs, run: !COMPOSE_CMD! logs -f
echo To stop the pipeline, run:   !COMPOSE_CMD! down
echo.

start http://localhost:8000
