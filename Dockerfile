# Use official Python image
FROM python:3.10

# Set the working directory
WORKDIR /app

# Copy application files
COPY . .

# Install dependencies
RUN pip install --upgrade pip && pip install -r requirements.txt

# Set environment variables for production
ENV UVICORN_HOST=0.0.0.0
ENV UVICORN_PORT=8000

# Expose API port
EXPOSE 8000

# Run FastAPI using Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]