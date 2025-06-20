# YouTube Trend Analysis Pipeline: ETL with Airflow, Spark, S3 and Docker

## Requirements

Python 3.10
Docker
Docker compose

## Project structure
```
    youtube-analysis/
    ├── .venv                   # Virtual environment for Python
    ├── dags/                   # Airflow DAGs for ETL pipeline
    ├── data/                   # Raw and processed data (git ignored)
    │   ├── raw/
    │   └── processed/
    ├── docker/                 # Docker-related files (Dockerfile, docker-compose.yml)
    ├── logs/                   # Log files for airflow
        └── scheduler/          # Scheduler logs
    ├── scripts/                # Data processing and utility scripts (PySpark, Python)
    ├── notebooks/              # Jupyter notebooks for exploration and analysis
    ├── tests/                  # Unit and integration tests
    ├── README.md
    └── requirements.txt        # Python dependencies
```

## Environment Setup

### Download Youtube dataset from Kaggle

Download dataset from [Kaggle](https://www.kaggle.com/datasets/datasnaek/youtube-new)

Extract the .zip file and paste all the files (10 csvs and 10 jsons) into /data/raw

### Create folder /logs/scheduler

Create the folder and provide enough privilege:

```bash
    mkdir -p ./logs/scheduler
    sudo chown -R 50000:0 ./logs
    sudo chmod -R 755 ./logs
```
 
### Set up development environment on Docker (Python, PySpark, Airflow)

- [x] postgreSQL
- [x] redis
- [x] localstack
- [x] airflow-webserver
- [x] airflow-scheduler
- [x] airflow-worker
- [x] pyspark-master
- [x] pyspark-worker

---

## Data Extraction & LocalStack Integration	

### Set up Localstack

### Set up LocalStack environment

---

## Configure Airflow-Localstack integration

### Set up PostgreSQL database for Airflow metadata

### Set up Airflow connection to S3

### Create DAG for S3 Integration

## Airflow setup for data ingestion

### Create Airflow DAG for the pipeline

### Define tasks for data processing and storage

### Set up Airflow monitoring and logging

