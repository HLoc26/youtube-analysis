#!/bin/bash

# Purpose: Automate the setup of the YouTube Trend Analysis Pipeline project

# Check for Docker
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install Docker and try again."
    exit 1
fi

# Check for Docker Compose
if ! command -v docker-compose &> /dev/null; then
    echo "Docker Compose is not installed. Please install Docker Compose and try again."
    exit 1
fi

# Check for Python 3.9
if ! command -v python &> /dev/null; then
    echo "Python 3.9 is not installed. Please install Python 3.9 and try again."
    exit 1
fi

# Install Python packages needed for init_localstack.py
echo "Installing required Python packages..."
python -m pip install boto3 python-dotenv mypy-boto3-s3

# Handle dataset
if [ ! -d "./data/raw" ] || [ -z "$(ls -A ./data/raw)" ]; then
    echo "Dataset not found in ./data/raw. Attempting to download..."
    if command -v kaggle &> /dev/null; then
        kaggle datasets download -d datasnaek/youtube-new
        if [ -f "youtube-new.zip" ]; then
            mkdir -p ./data/raw
            unzip youtube-new.zip -d ./data/raw
            rm youtube-new.zip
            echo "Dataset downloaded and extracted to ./data/raw"
        else
            echo "Failed to download dataset. Please ensure Kaggle CLI is configured correctly."
            echo "Alternatively, download the dataset manually from https://www.kaggle.com/datasets/datasnaek/youtube-new and extract it to ./data/raw"
            exit 1
        fi
    else
        echo "Kaggle CLI is not installed. Please install it or download the dataset manually."
        echo "To install Kaggle CLI: pip install kaggle"
        echo "Then, configure it with your Kaggle API token."
        echo "Alternatively, download the dataset from https://www.kaggle.com/datasets/datasnaek/youtube-new and extract it to ./data/raw"
        exit 1
    fi
else
    echo "Dataset found in ./data/raw. Skipping download."
fi

# Create logs directory with appropriate permissions
echo "Setting up logs directory..."
mkdir -p ./logs/scheduler
chown -R 50000:0 ./logs
chmod -R 755 ./logs

# Set up .env file and validate all fields
if [ ! -f ".env" ]; then
    cp .env.example .env
    echo ".env file created from .env.example."
    echo "Please edit .env and fill in all the required values listed in .env.example."
    echo "Required fields: POSTGRES_PASSWORD, POSTGRES_USER, POSTGRES_DB, POSTGRES_DB_CONN, REDIS_CONN, AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_ENDPOINT_URL, AIRFLOW_FERNET_KEY, AIRFLOW_WEBSERVER_SECRET_KEY, AIRFLOW_ADMIN_USERNAME, AIRFLOW_ADMIN_PASSWORD, AIRFLOW_ADMIN_FIRSTNAME, AIRFLOW_ADMIN_LASTNAME, AIRFLOW_ADMIN_EMAIL"
    echo "Press Enter to continue after editing .env."
    read
else
    echo ".env file already exists. Validating required fields..."
    required_vars=("POSTGRES_PASSWORD" "POSTGRES_USER" "POSTGRES_DB" "POSTGRES_DB_CONN" "REDIS_CONN" "AWS_DEFAULT_REGION" "AWS_ACCESS_KEY_ID" "AWS_SECRET_ACCESS_KEY" "S3_ENDPOINT_URL" "AIRFLOW_FERNET_KEY" "AIRFLOW_WEBSERVER_SECRET_KEY" "AIRFLOW_ADMIN_USERNAME" "AIRFLOW_ADMIN_PASSWORD" "AIRFLOW_ADMIN_FIRSTNAME" "AIRFLOW_ADMIN_LASTNAME" "AIRFLOW_ADMIN_EMAIL")
    missing_vars=()
    source .env
    for var in "${required_vars[@]}"; do
        if [ -z "${!var}" ]; then
            missing_vars+=("$var")
        fi
    done
    if [ ${#missing_vars[@]} -ne 0 ]; then
        echo "Error: The following required environment variables are missing or empty in .env:"
        for var in "${missing_vars[@]}"; do
            echo "  - $var"
        done
        echo "Please fill in these values in .env and try again."
        exit 1
    fi
    echo "All required .env variables are set."
fi

# Build and start Docker containers
echo "Building and starting Docker containers..."
docker compose build
docker compose up -d

# Wait for Airflow webserver to be ready (ensures entrypoint.sh has run)
echo "Waiting for Airflow webserver to be ready..."
until curl -f http://localhost:8080/health > /dev/null 2>&1; do
    echo "Airflow not yet available, waiting 5 seconds..."
    sleep 5
done
echo "Airflow is ready. Database and admin user initialized via entrypoint.sh."

# Wait for LocalStack to be ready
echo "Waiting for LocalStack to be ready..."
until curl -f http://localhost:4566/_localstack/health > /dev/null 2>&1; do
    echo "LocalStack not yet available, waiting 5 seconds..."
    sleep 5
done
echo "LocalStack is ready."

# # Initialize LocalStack S3 buckets and upload data
# echo "Initializing LocalStack S3 with init_localstack.py..."
# python scripts/init_localstack.py

# Check status of all services
echo "Checking status of all services..."
docker ps

# Provide completion message with access instructions
echo "Setup completed successfully!"
echo "Access the following services:"
echo "  - Airflow Web Interface: http://localhost:8080"
echo "  - Spark Master UI: http://localhost:9090"
echo "  - LocalStack S3 Endpoint: http://localhost:4566"
echo "  - JupyterLab (Data Visualization): http://localhost:8888"
echo "Useful commands:"
echo "  - View all container logs: docker-compose logs -f"
echo "  - Stop all services: docker-compose down"
echo "  - List Airflow DAGs: docker exec -it youtube_airflow_web airflow dags list"
echo "  - Access Airflow CLI: docker exec -it youtube_airflow_web airflow [command]"
echo "  - Check LocalStack S3 buckets: aws --endpoint-url=http://localhost:4566 s3 ls"