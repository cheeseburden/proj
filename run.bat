@echo off
echo ====================================================================
echo        Starting Fake News Classification Pipeline
echo        Kafka ^| Hadoop HDFS ^| TF-IDF NLP ^| Docker
echo ====================================================================

where docker >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Docker is not installed or not in PATH. Please install Docker Desktop.
    exit /b 1
)

echo.
echo [1/3] Checking Docker Compose setup...
docker compose version >nul 2>nul
if %ERRORLEVEL% neq 0 (
    set COMPOSE_CMD=docker-compose
) else (
    set COMPOSE_CMD=docker compose
)

echo [2/3] Building and starting all pipeline services...
%COMPOSE_CMD% up --build -d

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
echo To view real-time logs, run: %COMPOSE_CMD% logs -f
echo To stop the pipeline, run:   %COMPOSE_CMD% down
echo.

start http://localhost:8000
