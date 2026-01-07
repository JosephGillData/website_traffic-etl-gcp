# Traffic Data ETL Pipeline

A production-ready ETL pipeline that extracts traffic data from Excel files, transforms it for analysis, and loads it into Google BigQuery via Google Cloud Storage using [Google Cloud Console](https://console.cloud.google.com/)

## Overview

This pipeline:
1. **Extracts** data from an XLS spreadsheet
2. **Transforms** the data (reformats timestamps, adds metadata, validates)
3. **Loads** both a backup of the original file and the processed CSV to GCS, then into BigQuery

## Quick Start

```bash
# Clone and enter the repository
git clone <repo-url>
cd sky-task-1

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies and the ETL package (editable mode)
pip install -r requirements.txt
pip install -e .

# Copy and configure environment variables
cp .env.example .env
# Edit .env with your GCP settings (see Configuration section)

# Validate configuration
python -m etl validate

# Run the ETL pipeline
python -m etl run
```

## Requirements

- Python 3.10+
- Google Cloud Platform account with:
  - A GCP project with billing enabled
  - A GCS bucket
  - A BigQuery dataset
  - A service account with appropriate permissions

## Installation

### Windows

```powershell
# Create virtual environment
python -m venv venv
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# For development
pip install -r requirements-dev.txt
```

### macOS / Linux

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# For development
pip install -r requirements-dev.txt
```

## Configuration

### Environment Variables

Create a `.env` file (copy from `.env.example`):

```bash
# Required
GCP_PROJECT=your-gcp-project-id
GCS_BUCKET=your-gcs-bucket-name
BQ_DATASET=your_bigquery_dataset
BQ_TABLE=traffic_data
LOCAL_XLS_PATH=data/traffic_spreadsheet.xls
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account-key.json

# Optional
BQ_WRITE_DISPOSITION=append  # or "truncate"
```

### GCP Service Account Setup

1. Go to [GCP Console > IAM & Admin > Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts)

2. Click **Create Service Account**:
   - Name: `etl-pipeline` (or your choice)
   - Description: "Service account for traffic ETL pipeline"

3. Grant the following roles:
   - `Storage Object Admin` (for GCS uploads)
   - `BigQuery Data Editor` (for loading data)
   - `BigQuery Job User` (for running load jobs)

4. Create and download a JSON key:
   - Click on the service account
   - Go to **Keys** tab
   - **Add Key** > **Create new key** > **JSON**
   - Save the file securely (never commit to git!)

5. Set the path in your `.env`:
   ```
   GOOGLE_APPLICATION_CREDENTIALS=/path/to/your-key.json
   ```

### Create GCS Bucket

```bash
# Using gcloud CLI
gcloud storage buckets create gs://your-bucket-name --location=US

# Or via Console: https://console.cloud.google.com/storage/browser
```

### Create BigQuery Dataset

```bash
# Using bq CLI
bq mk --dataset your-project:your_dataset

# Or via Console: https://console.cloud.google.com/bigquery
```

## Usage

### CLI Commands

```bash
# Validate configuration (doesn't run ETL)
python -m etl validate

# Run ETL pipeline (appends to existing data)
python -m etl run

# Run with verbose logging
python -m etl run --verbose

# Run and truncate (replace) existing BigQuery data
python -m etl run --truncate
```

### Using Make

```bash
make install      # Install dependencies
make install-dev  # Install dev dependencies
make test         # Run tests
make run          # Run ETL
make validate     # Validate config
```

## Pipeline Output

### GCS Files

The pipeline uploads to your GCS bucket:
- `backups/original_YYYYMMDD_HHMMSS.xls` - Backup of source file
- `processed/traffic_data_YYYYMMDD_HHMMSS.csv` - Processed CSV

### BigQuery Schema

| Column | Type | Description |
|--------|------|-------------|
| `time` | TIMESTAMP | Observation timestamp (UTC assumed) |
| `traffic` | FLOAT64 | Traffic volume in Gbps |
| `created_at` | TIMESTAMP | ETL processing timestamp (UTC) |

### Data Transformations

- **Time format**: Input `dd/mm/YY HH:MM` → Output `YYYY-mm-dd HH:MM:SS`
- **Timezone**: All timestamps treated as UTC (source has no timezone info)
- **created_at**: Added as current UTC timestamp at processing time

## Automation

### Local Cron (Linux/macOS)

Edit crontab with `crontab -e`:

```cron
# Run ETL daily at midnight
0 0 * * * cd /path/to/sky-task-1 && /path/to/sky-task-1/venv/bin/python -m etl run >> /var/log/etl.log 2>&1
```

Full example with absolute paths:
```cron
0 0 * * * cd /home/user/sky-task-1 && /home/user/sky-task-1/venv/bin/python -m etl run >> /home/user/logs/etl.log 2>&1
```

### Windows Task Scheduler

1. Open Task Scheduler
2. Create Basic Task
3. Set trigger: Daily at desired time
4. Action: Start a program
   - Program: `C:\path\to\venv\Scripts\python.exe`
   - Arguments: `-m etl run`
   - Start in: `C:\path\to\sky-task-1`

### Cloud-Native Alternative (Recommended)

For production, consider **Cloud Scheduler + Cloud Run Jobs**:

1. **Containerize** the ETL:
   ```dockerfile
   FROM python:3.11-slim
   WORKDIR /app
   COPY requirements.txt .
   RUN pip install -r requirements.txt
   COPY src/ src/
   COPY data/ data/
   CMD ["python", "-m", "etl", "run"]
   ```

2. **Deploy to Cloud Run Jobs**:
   ```bash
   gcloud run jobs create etl-pipeline \
     --image gcr.io/YOUR_PROJECT/etl-pipeline \
     --set-env-vars GCP_PROJECT=...,GCS_BUCKET=...,BQ_DATASET=...,BQ_TABLE=...
   ```

3. **Schedule with Cloud Scheduler**:
   ```bash
   gcloud scheduler jobs create http etl-daily \
     --schedule="0 0 * * *" \
     --uri="https://REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/PROJECT/jobs/etl-pipeline:run" \
     --http-method=POST \
     --oauth-service-account-email=SERVICE_ACCOUNT@PROJECT.iam.gserviceaccount.com
   ```

**Alternative: Cloud Functions**
- Trigger: Cloud Scheduler via Pub/Sub
- Good for lightweight, stateless ETL
- Consider memory/timeout limits for larger datasets

## Looker Studio Dashboard

### Connecting to BigQuery

1. Go to [Looker Studio](https://lookerstudio.google.com/)
2. Click **Create** > **Data source**
3. Select **BigQuery**
4. Navigate to your project > dataset > table
5. Click **Connect**

### Recommended Dashboard Configuration

**Traffic Over Time Chart:**
- Chart type: **Time series** or **Line chart**
- Dimension: `time` (set as Date Hour)
- Metric: `traffic` (set as Average or Sum)
- Sort: `time` ascending

**Suggested Visualizations:**
1. **Line chart**: Traffic volume over time
2. **Scorecard**: Average traffic, Peak traffic
3. **Table**: Detailed data with time, traffic, created_at
4. **Bar chart**: Traffic by hour of day (use EXTRACT(HOUR FROM time))

**Date Range Control:**
- Add a date range control filtered on `time` column
- Set default to "Last 7 days" or "Custom"

### Sample Dashboard Query

For hourly aggregation in BigQuery:
```sql
SELECT
  TIMESTAMP_TRUNC(time, HOUR) AS hour,
  AVG(traffic) AS avg_traffic,
  MAX(traffic) AS max_traffic,
  MIN(traffic) AS min_traffic
FROM `project.dataset.traffic_data`
GROUP BY hour
ORDER BY hour
```

## Troubleshooting

### 403 Permission Denied

**Symptom:** `google.api_core.exceptions.Forbidden: 403`

**Solutions:**
1. Verify service account has required roles:
   - `Storage Object Admin` for GCS
   - `BigQuery Data Editor` and `BigQuery Job User` for BigQuery
2. Check the bucket/dataset exists in the correct project
3. Ensure `GOOGLE_APPLICATION_CREDENTIALS` points to correct file
4. Verify the service account belongs to the same project

### Bucket Not Found

**Symptom:** `google.api_core.exceptions.NotFound: 404 ... bucket`

**Solutions:**
1. Create the bucket: `gsutil mb gs://your-bucket-name`
2. Verify bucket name in `.env` (no `gs://` prefix)
3. Check bucket is in the correct GCP project

### Billing Not Enabled

**Symptom:** `BigQuery: Access Denied: BigQuery BigQuery: Billing has not been enabled`

**Solution:**
1. Enable billing at [GCP Billing](https://console.cloud.google.com/billing)
2. Link billing account to your project

### Schema Mismatch

**Symptom:** `Schema field ... has changed type`

**Solutions:**
1. Use `--truncate` flag to replace table: `python -m etl run --truncate`
2. Delete and recreate the table in BigQuery Console
3. Ensure you're not mixing data from different sources

### Missing Credentials

**Symptom:** `ConfigError: Google credentials file not found`

**Solutions:**
1. Download key from GCP Console > IAM > Service Accounts
2. Set absolute path in `GOOGLE_APPLICATION_CREDENTIALS`
3. Verify file permissions are readable

### XLS File Not Found

**Symptom:** `ConfigError: XLS file not found`

**Solutions:**
1. Verify `LOCAL_XLS_PATH` in `.env`
2. Use absolute path if relative path fails
3. Check file exists: `ls -la data/traffic_spreadsheet.xls`

## Development

### Running Tests

```bash
# Run all tests
make test

# Run with coverage
make test-cov

# Run specific test file
pytest tests/test_transform.py -v
```

### Code Quality

```bash
# Lint code
make lint

# Format code
make format

# Install pre-commit hooks
pre-commit install
```

### Project Structure

```
sky-task-1/
├── src/
│   └── etl/
│       ├── __init__.py
│       ├── __main__.py      # CLI entrypoint
│       ├── config.py        # Configuration management
│       ├── extract.py       # Data extraction
│       ├── transform.py     # Data transformation
│       ├── load.py          # GCS/BigQuery loading
│       └── logging_config.py
├── tests/
│   ├── test_config.py
│   ├── test_extract.py
│   └── test_transform.py
├── data/
│   └── traffic_spreadsheet.xls
├── .env.example
├── .gitignore
├── Makefile
├── README.md
├── requirements.txt
├── requirements-dev.txt
└── pyproject.toml
```

## The Dataset

The source data (`data/traffic_spreadsheet.xls`) contains network traffic observations:

| Column | Description |
|--------|-------------|
| `time` | Timestamp of observation (dd/mm/YY HH:MM format) |
| `traffic` | Traffic volume, likely in Gbps |

Sample data covers 287 observations at 5-minute intervals.

## License

MIT

## Author

Joseph Gill
- [Website](https://joegilldata.com)
- [LinkedIn](https://www.linkedin.com/in/joseph-gill-726b52182/)
