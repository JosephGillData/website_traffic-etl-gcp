Load the variables into your shell (so Docker Compose can access them):

```bash
set -a
source .env
set +a
```

## Setup (End-to-End)

#### Clone the Repository

```bash
cd ~
git clone git@github.com:JosephGillData/website-traffic-etl-airflow-dev.git
cd website-traffic-etl-airflow-dev
```

#### Create and Activate Virtual Environment

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

#### Authenticate with GCP

```bash
gcloud auth login
gcloud auth application-default login
```

When you login, the location of your credentials will be printed in the command line. Update the .env file with this value.

#### Create/select a GCP project

If you already have a project, skip creation.

```bash
gcloud projects create "$PROJECT_ID" --name="INSERT_PROJECT_NAME"
```

Set it as your active project:

```bash
gcloud config set project "$PROJECT_ID"
```

Link billing (requires permissions on the billing account + project):

```bash
gcloud billing projects link "$PROJECT_ID" --billing-account="$BILLING_ACCOUNT_ID"
```

Set quota project for ADC (helps avoid “quota project” / ADC warnings and misbilling):

```bash
gcloud auth application-default set-quota-project "$PROJECT_ID"
```
