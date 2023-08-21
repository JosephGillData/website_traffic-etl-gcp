# %%
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
from google.cloud import storage
import os
from google.cloud import bigquery
from datetime import datetime

# %%
current_directory = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = current_directory + "/canvas-radio-396115-06ab17b6af40.json"

os.chdir(current_directory)

today = datetime.now().strftime('%y-%m-%d %H:%M')
df = pd.read_excel('./data/traffic_spreadsheet.xls')
df['time'] = df['time'].dt.strftime('%y-%m-%d %H:%M')
df['created_at'] = today
df.to_csv('data/traffic_spreadsheet.csv', index=False, header=False)

# %%
# Create a client using the service account credentials
client = storage.Client()

# Replace 'your-bucket-name' with your GCS bucket's name
bucket_name = 'sky_traffic_data'
bucket = client.bucket(bucket_name)

# Path to the local file you want to upload
local_file_path = 'data/traffic_spreadsheet.csv'


# Destination blob name in the bucket
destination_blob_name = f"traffic_spreadsheet_{today}.csv"

# Upload the local file to the bucket
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(local_file_path)

print(f'File {local_file_path} uploaded to {bucket_name}/{destination_blob_name}')

# %%
# Set up the BigQuery client
bq_client = bigquery.Client()

# Set up the GCS bucket and object name
gcs_uri = f"gs://sky_traffic_data/traffic_spreadsheet_{today}.csv"

# Set up the BigQuery dataset and table name
dataset_id = 'sky_transport_dataset'
table_id = 'sky_transport_table'

# Configure the job to load data
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV

# Load data from GCS to BigQuery
load_job = bq_client.load_table_from_uri(gcs_uri, dataset_id + '.' + table_id, job_config=job_config)

# Wait for the job to complete
print(load_job.result())

# %%
