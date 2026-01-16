# Website Traffic ETL (GCP)

An end-to-end **Data Engineering ETL pipeline** built with **Python**, integrating with **Google Cloud Storage (GCS)** and **BigQuery** on **Google Cloud Platform (GCP)**.

This repo is built as a **portfolio project for Data Engineering interviews** and focuses on demonstrating real-world patterns:

- Clean ETL architecture (Extract, Transform, Load modules)
- GCP authentication via **Application Default Credentials (ADC)**
- Secure configuration via `.env` environment variables
- Cloud Storage ingestion (XLS in → pipeline processes it → BigQuery)
- CLI-first setup, reproducible local runs

## What this pipeline does

At a high level, the ETL:

1. **Extracts** website traffic data from an XLS file stored in **Google Cloud Storage**
2. **Transforms** the dataset by:
   - Parsing dates to ISO format
   - Adding `created_at` timestamp
   - Validating required columns and data types
3. **Loads** the data:
   - Backs up the original file in GCS
   - Uploads processed CSV to GCS
   - Loads into **BigQuery** (append or truncate mode)

This mirrors a common production pattern:

> Cloud object storage → ETL ingestion → transformation → data warehouse

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Google Cloud Platform                        │
│                                                                     │
│  ┌─────────────────┐    ┌─────────────┐    ┌─────────────────────┐ │
│  │   GCS Bucket    │    │  Local ETL  │    │      BigQuery       │ │
│  │                 │    │             │    │                     │ │
│  │  raw_data/      │───▶│  Extract    │    │  website_traffic    │ │
│  │    └─ .xls      │    │     ↓       │    │    └─ traffic_data  │ │
│  │                 │    │  Transform  │    │                     │ │
│  │  processed/     │◀───│     ↓       │───▶│  time | traffic |   │ │
│  │    └─ .csv      │    │  Load       │    │  created_at         │ │
│  │                 │    │             │    │                     │ │
│  │  backups/       │    └─────────────┘    └─────────────────────┘ │
│  │    └─ .xls      │                                               │
│  └─────────────────┘                                               │
└─────────────────────────────────────────────────────────────────────┘
```

### Local
- Python CLI runs the ETL pipeline
- Authentication uses **ADC** (Application Default Credentials)
- Configuration loaded from `.env` file

### Cloud (GCP)
- Source data lives in **GCS** (`raw_data/`)
- Processed CSVs stored in **GCS** (`processed/`)
- Backups stored in **GCS** (`backups/`)
- Final data loaded into **BigQuery**

## Tech Stack

- Python 3.10+
- pandas (data processing)
- Google Cloud Platform
  - Cloud Storage
  - BigQuery
  - gcloud CLI
- Linux / CLI-first workflow

## Prerequisites

You must have the following installed locally:

- Python 3.10+
- Google Cloud SDK (`gcloud`)
- A GCP project with billing enabled

## Configuration

Create a `.env` file from the example:

```bash
cp .env.example .env
```

Edit the variables:

| Variable               | Description                                              | Used by             |
| ---------------------- | -------------------------------------------------------- | ------------------- |
| `PROJECT_ID`           | Google Cloud project ID (e.g. `website-traffic-etl-dev`) | GCS, BigQuery       |
| `GCS_BUCKET`           | Name of the GCS bucket (must be globally unique)         | Extract, Load       |
| `BQ_DATASET`           | BigQuery dataset name                                    | Load                |
| `BQ_TABLE`             | BigQuery table name                                      | Load                |
| `BQ_WRITE_DISPOSITION` | `append` or `truncate` (default: `append`)               | Load                |

## Setup (End-to-End)

### Clone the Repository

```bash
cd ~
git clone git@github.com:JosephGillData/website-traffic-etl-gcp.git
cd website-traffic-etl-gcp
```

### Create and Activate Virtual Environment

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

### Authenticate with GCP

```bash
gcloud auth login
gcloud auth application-default login
```

### Create/select a GCP project

If you already have a project, skip creation.

```bash
gcloud projects create "$PROJECT_ID" --name="Website Traffic ETL"
```

Set it as your active project:

```bash
gcloud config set project "$PROJECT_ID"
```

Link billing (requires permissions on the billing account + project):

```bash
gcloud billing projects link "$PROJECT_ID" --billing-account="$BILLING_ACCOUNT_ID"
```

Set quota project for ADC (helps avoid "quota project" / ADC warnings):

```bash
gcloud auth application-default set-quota-project "$PROJECT_ID"
```

### Create a GCS bucket and upload the XLS

```bash
gcloud storage buckets create "gs://$GCS_BUCKET" --location=EU --uniform-bucket-level-access

gcloud storage cp ./data/traffic_spreadsheet.xls "gs://$GCS_BUCKET/raw_data/traffic_spreadsheet.xls"
```

### Create BigQuery dataset

```bash
bq mk --dataset --location=EU "$PROJECT_ID:$BQ_DATASET"
```

## Run the Pipeline

```bash
python -m etl run
```

### Options

| Flag         | Description                              |
| ------------ | ---------------------------------------- |
| `--truncate` | Replace table data instead of appending  |

### Example output

```
2024-01-15 10:30:00 [INFO] Starting ETL pipeline
2024-01-15 10:30:00 [INFO] Project: my-project, Bucket: my-bucket
2024-01-15 10:30:00 [INFO] === EXTRACT ===
2024-01-15 10:30:01 [INFO] Downloading gs://my-bucket/raw_data/traffic_spreadsheet.xls
2024-01-15 10:30:02 [INFO] Extracted 1000 rows
2024-01-15 10:30:02 [INFO] === TRANSFORM ===
2024-01-15 10:30:02 [INFO] Transformed 1000 rows
2024-01-15 10:30:02 [INFO] === LOAD ===
2024-01-15 10:30:03 [INFO] Copying to backups/original_20240115_103000.xls
2024-01-15 10:30:04 [INFO] Uploading to gs://my-bucket/processed/traffic_data_20240115_103000.csv
2024-01-15 10:30:05 [INFO] Loading into my-project.website_traffic.traffic_data
2024-01-15 10:30:07 [INFO] Loaded 1000 rows into BigQuery
2024-01-15 10:30:07 [INFO] === COMPLETE: 1000 rows loaded ===
```

## Project Structure

```
website-traffic-etl-gcp/
├── data/
│   └── traffic_spreadsheet.xls   # Sample source data
├── src/
│   └── etl/
│       ├── __init__.py
│       ├── __main__.py           # CLI entrypoint
│       ├── config.py             # Configuration from .env
│       ├── extract.py            # Download XLS from GCS
│       ├── transform.py          # Parse, validate, enrich
│       └── load.py               # Upload to GCS, load to BigQuery
├── .env.example
├── pyproject.toml
├── requirements.txt
└── README.md
```
