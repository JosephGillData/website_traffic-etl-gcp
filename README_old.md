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
PROJECT_ID=your-gcp-project-id
GCS_BUCKET=your-gcs-bucket-name
INPUT_GCS_URI=gs://your-gcs-bucket-name/raw_data/traffic_spreadsheet.xls
BQ_DATASET=your_bigquery_dataset
BQ_TABLE=traffic_data

# Optional - for local development only (not needed for Cloud Run)
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account-key.json

# Optional
BQ_WRITE_DISPOSITION=append  # or "truncate"
```

### Authentication (Application Default Credentials)

This pipeline uses **Application Default Credentials (ADC)** for authentication.
The GCP client libraries automatically discover credentials in this order:

| Environment | How Authentication Works |
|------------|-------------------------|
| **Cloud Run Jobs** | Automatic - uses attached service account identity. **No JSON key files needed.** |
| **Local (gcloud CLI)** | Run `gcloud auth application-default login` |
| **Local (JSON key)** | Set `GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json` in `.env` |

**Important for Cloud Run:**
- Do NOT set `GOOGLE_APPLICATION_CREDENTIALS` in Cloud Run
- Do NOT bake JSON key files into container images
- Instead, attach a service account to the Cloud Run Job
- The `.dockerignore` excludes `.env` and `*.json` to prevent accidental credential leaks

### GCP Service Account Setup

1. Go to [GCP Console > IAM & Admin > Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts)

2. Click **Create Service Account**:
   - Name: `etl-pipeline` (or your choice)
   - Description: "Service account for traffic ETL pipeline"

3. Grant the following roles:
   - `Storage Object Admin` (for GCS uploads and downloads)
   - `BigQuery Data Editor` (for loading data)
   - `BigQuery Job User` (for running load jobs)

4. **For Local Development Only** - Create a JSON key:
   - Click on the service account
   - Go to **Keys** tab
   - **Add Key** > **Create new key** > **JSON**
   - Save the file securely (never commit to git!)
   - Set the path in your `.env`:
     ```
     GOOGLE_APPLICATION_CREDENTIALS=/path/to/your-key.json
     ```

5. **For Cloud Run** - Attach the service account to the job (no JSON key needed)

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

1. **Build and push the container image**:
   ```bash
   # Build the image
   docker build -t gcr.io/YOUR_PROJECT/etl-pipeline .

   # Push to Container Registry
   docker push gcr.io/YOUR_PROJECT/etl-pipeline
   ```

2. **Deploy to Cloud Run Jobs** (with service account for ADC):
   ```bash
   gcloud run jobs create etl-pipeline \
     --image gcr.io/YOUR_PROJECT/etl-pipeline \
     --region us-central1 \
     --service-account etl-pipeline@YOUR_PROJECT.iam.gserviceaccount.com \
     --set-env-vars "PROJECT_ID=YOUR_PROJECT,GCS_BUCKET=your-bucket,INPUT_GCS_URI=gs://your-bucket/raw_data/traffic_spreadsheet.xls,BQ_DATASET=your_dataset,BQ_TABLE=traffic_data"
   ```

   **Important:** The `--service-account` flag attaches the service account identity.
   The pipeline uses ADC to authenticate automatically - no JSON key files needed!

3. **Schedule with Cloud Scheduler**:
   ```bash
   gcloud scheduler jobs create http etl-daily \
     --schedule="0 0 * * *" \
     --uri="https://REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/PROJECT/jobs/etl-pipeline:run" \
     --http-method=POST \
     --oauth-service-account-email=SERVICE_ACCOUNT@PROJECT.iam.gserviceaccount.com
   ```

### Temporary Files in Cloud Run

The pipeline uses Python's `tempfile` module for intermediate files:
- Downloaded XLS files from GCS are stored in temp directory
- Processed CSV files are created in temp directory before upload
- Cloud Run provides `/tmp` as a writable temp directory (in-memory)
- All temp files are automatically cleaned up after use
- If cleanup fails, a warning is logged but the pipeline continues

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

### Authentication Failed

**Symptom:** `Failed to create GCS client` or `Authentication failed`

**Solutions for Local Development:**
1. Run `gcloud auth application-default login` (recommended)
2. OR download a JSON key from GCP Console > IAM > Service Accounts and set `GOOGLE_APPLICATION_CREDENTIALS` in `.env`

**Solutions for Cloud Run:**
1. Ensure the Cloud Run Job has a service account attached (`--service-account` flag)
2. Verify the service account has required IAM roles
3. Do NOT set `GOOGLE_APPLICATION_CREDENTIALS` - let ADC handle it

### Source File Not Found in GCS

**Symptom:** `Source file not found in GCS: gs://bucket/path`

**Solutions:**
1. Verify `INPUT_GCS_URI` in `.env` points to correct GCS path
2. Upload the source XLS file to GCS: `gsutil cp data/traffic_spreadsheet.xls gs://your-bucket/raw_data/`
3. Check file exists: `gsutil ls gs://your-bucket/raw_data/`

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



## Running the code

Clone the repo and setup etc.

### Google Cloud Setup

Create a new project in Google Cloud

#### Cloud Storage Setup

* Create a new Cloud Storage bucket called website_analysis.
* Upload the traffic_spreahsheet.xls file.


#### BigQuery Setup

Create a new BigQuery dataset called website_analysis
Within that dataset, create a table called website_traffic with columns:
* time (TIMESTAMP - REQUIRED)
* traffic (FLOAT - REQUIRED)
* created_at (TIMESTAMP - REQUIRED)

pip install the module in an editable way (reacts to live changes)
pip install -e .

Run the ETL pipeline
python -m etl
