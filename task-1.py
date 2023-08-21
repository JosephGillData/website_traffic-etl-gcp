# %%
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
from google.cloud import storage
import os
from google.cloud import bigquery
import datetime

# %%
print('')
print('')
print('')
print('')
# Get the current time
current_time = datetime.datetime.now()
# Format the current time as YYYY-MM-DD HH:MM:SS
formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
# Print the formatted time
print(formatted_time)
print('')
print('')

current_directory = os.getcwd()
print("Current Working Directory:", current_directory)

# Get the directory of the currently executing script
script_directory = os.path.dirname(os.path.abspath(__file__))

print(script_directory + "/canvas-radio-396115-06ab17b6af40.json")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = script_directory + "/canvas-radio-396115-06ab17b6af40.json"

# Change the current working directory to the script's directory
os.chdir(script_directory)

# Print the updated current working directory
print("Current Working Directory:", os.getcwd())

# upload
df = pd.read_excel('./data/traffic_spreadsheet.xls')

# process
df['time'] = df['time'].dt.strftime('%y-%m-%d %H:%M')

# convert to csv
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
destination_blob_name = 'traffic_spreadsheet.csv'

# Upload the local file to the bucket
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(local_file_path)

print(f'File {local_file_path} uploaded to {bucket_name}/{destination_blob_name}')

# %%
print('1')

# Set up the BigQuery client
bq_client = bigquery.Client()

# Set up the GCS bucket and object name
gcs_uri = 'gs://sky_traffic_data/traffic_spreadsheet.csv'

# Set up the BigQuery dataset and table name
dataset_id = 'sky_transport_dataset'
table_id = 'sky_transport_table'

print('2')

# Configure the job to load data
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV

print('3')

# Load data from GCS to BigQuery
load_job = bq_client.load_table_from_uri(gcs_uri, dataset_id + '.' + table_id, job_config=job_config)

print('4')

# Wait for the job to complete
print(load_job.result())

('5')

# %%
