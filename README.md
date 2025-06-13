# YouTube Trend Analysis Pipeline: ETL with Airflow, Spark, S3 and Docker

## Requirements

Python 3.10
Docker
Docker compose

## Project structure
```
    youtube-analysis/
    ├── dags/                   # Airflow DAGs for ETL pipeline
    ├── docker/                 # Docker-related files (Dockerfile, docker-compose.yml)
    ├── scripts/                # Data processing and utility scripts (PySpark, Python)
    ├── data/                   # Raw and processed data (git ignored)
    │   ├── raw/
    │   └── processed/
    ├── notebooks/              # Jupyter notebooks for exploration and analysis
    ├── configs/                # Configuration files (Airflow, Spark, LocalStack, etc.)
    ├── tests/                  # Unit and integration tests
    ├── README.md
    ├── requirements.txt        # Python dependencies
    ├── airflow.cfg             # Airflow configuration (if not using env vars)
    └── .env                    # Environment variables (Docker, Airflow, etc.)
```

## Environment Setup

### Download Youtube dataset from Kaggle

Download dataset from [Kaggle](https://www.kaggle.com/datasets/datasnaek/youtube-new)

Extract the .zip file and paste all the files (10 csvs and 10 jsons) into /data/raw
 
### Set up development environment on Docker (Python, PySpark, Airflow)

- [x] postgreSQL
- [x] redis
- [x] localstack
- [ ] airflow-webserver
- [ ] airflow-scheduler
- [ ] airflow-worker
- [ ] pyspark-master
- [ ] pyspark-worker

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

