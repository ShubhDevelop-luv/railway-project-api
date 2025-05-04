#!/bin/bash
# Start FastAPI & Celery (Linux/Mac)
#!/bin/bash
echo "Starting FastAPI server..."

# Activate virtual environment (optional)
source env/bin/activate

# Run Uvicorn server on Azure App Service
uvicorn main:app --host 0.0.0.0 --port 8000 --reload --workers 1 --timeout-keep-alive 60 --timeout-request 60 &
# Run Celery worker with Redis as the broker
celery -A main.celery worker --loglevel=info --concurrency=1 &  