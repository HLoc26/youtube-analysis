# YouTube Trend Analysis Pipeline: ETL with Airflow, Spark, S3 and Docker

## Overview

This project provides a complete data pipeline for analyzing YouTube trending videos across multiple countries. It leverages Apache Airflow for orchestration, Apache Spark for distributed data processing, and LocalStack to emulate AWS S3 locally for development and testing. The pipeline extracts, validates, processes, and analyzes YouTube trending data, producing analytics and insights.

### Key Features

- **Automated ETL Pipeline**: Orchestrated with Airflow, from raw data ingestion to analytics report generation.
- **Distributed Processing**: Uses Spark for scalable data transformation and aggregation.
- **Local AWS Emulation**: LocalStack provides a local S3-compatible environment, so no real AWS account is needed.
- **Data Validation**: Ensures data quality and schema compliance before processing.
- **Extensible**: Easily add new data sources, transformations, or analytics.

---

## Project Structure

```
.
├── dags/               # Airflow DAG definitions
├── data/               # Raw and processed data storage
│   ├── processed/
│   └── raw/
├── dockerfiles/        # Dockerfiles for Airflow, Spark, Jupyter
├── jobs/               # Spark jobs/scripts
├── logs/               # Airflow logs
├── notebooks/          # Jupyter notebooks for data exploration
├── scripts/            # Helper scripts (e.g., LocalStack init)
├── config/             # Spark configuration files
├── .env                # Environment variable definitions
├── docker-compose.yml  # Container orchestration
└── requirements.txt    # Python dependencies
```

---

## What Can This Project Do?

- **Ingest** YouTube trending datasets (CSV/JSON) for multiple countries.
- **Validate** data for schema, types, and quality issues.
- **Process** data using Spark (e.g., aggregations, analytics).
- **Store** processed and analytics data in S3 (LocalStack).
- **Generate** analytics reports (top categories, channels, trends).
- **Explore** data interactively in JupyterLab.

---

## Prerequisites

- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)
- Python 3.9 (for local scripts, if needed)
- [Kaggle account](https://www.kaggle.com/) (to download the YouTube dataset)

---

## Environment Setup

### 1. Clone the Repository

```sh
git clone https://github.com/HLoc26/youtube-analysis.git
cd youtube-analysis
```

### 2. Prepare the Dataset

- Download the [YouTube Trending Dataset from Kaggle](https://www.kaggle.com/datasets/datasnaek/youtube-new).
- Extract all CSV and JSON files into `./data/raw/`.

### 3. Configure Environment Variables

- Copy `.env.example` to `.env` and fill in all required values.
- **Important:** You must set strong values for `AIRFLOW_FERNET_KEY` and `AIRFLOW_WEBSERVER_SECRET_KEY`.

#### Generate Secure Keys

You can generate secure keys using Python:

```sh
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

- Set the output as `AIRFLOW_FERNET_KEY` in your `.env`.
- For `AIRFLOW_WEBSERVER_SECRET_KEY`, you can use a random string (at least 32 chars), e.g.:

```sh
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

- Example `.env` entries:

```
AIRFLOW_FERNET_KEY="your-generated-fernet-key"
AIRFLOW_WEBSERVER_SECRET_KEY="your-generated-secret-key"
```

### 4. Build and Start All Services

```sh
./scripts/setup.sh
```

This script will:
- Validate your `.env` file.
- Build Docker images.
- Start all containers (Postgres, Redis, LocalStack, Airflow, Spark, Jupyter).
- Wait for services to be healthy.

---

## Service Access

- **Airflow Web UI**: [http://localhost:8080](http://localhost:8080)
- **Spark Master UI**: [http://localhost:9090](http://localhost:9090)
- **JupyterLab**: [http://localhost:8888](http://localhost:8888) (token: `youtube123`)
- **LocalStack S3**: [http://localhost:4566](http://localhost:4566)

---

## Running the Pipeline

1. **Upload Data**: The Airflow DAG will upload raw data to LocalStack S3.
2. **Validate Data**: Data is validated for schema and quality.
3. **Process Data**: Spark jobs process and aggregate the data.
4. **Generate Analytics**: Analytics reports are produced and stored in S3.
5. **Explore Results**: Use Jupyter notebooks for further analysis and visualization.

---

## Troubleshooting

- Ensure all required environment variables are set in `.env`.
- If you change `.env`, restart the containers.
- Check logs in the `logs/` directory or via `docker-compose logs`.

---

## License

MIT License

---

## Acknowledgements

- [Kaggle YouTube Trending Dataset](https://www.kaggle.com/datasets/datasnaek/youtube-new)
- [LocalStack](https://github.com/localstack/localstack)
- [Apache Airflow](https://airflow.apache.org/)
- [Apache Spark](https://spark.apache.org/)