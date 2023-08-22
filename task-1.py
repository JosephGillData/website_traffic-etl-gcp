# %%
# import packages

import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
from google.cloud import storage
import os
from google.cloud import bigquery
from datetime import datetime

# %%
# set working directory and GOOGLE_APPLICATION_CREDENTIALS env variable

current_directory = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = current_directory + "/canvas-radio-396115-06ab17b6af40.json"
os.chdir(current_directory)

# %%
# convert the .xls file to .csv

today = datetime.now().strftime('%y-%m-%d %H:%M')
df = pd.read_excel('./data/traffic_spreadsheet.xls')
df['time'] = df['time'].dt.strftime('%y-%m-%d %H:%M')
df['created_at'] = today
df.to_csv('data/traffic_spreadsheet.csv', index=False, header=False)

# %%
# upload the traffic_spreadsheet.csv file to a Google Cloud Storage

bucket_name = 'sky_traffic_data'
destination_blob_name = f"traffic_spreadsheet_{today}.csv" # i added todays date so that files aren't overwritten on upload
local_file_path = 'data/traffic_spreadsheet.csv' # data we are uploading

client = storage.Client() # create a storage bucket client
bucket = client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(local_file_path) # upload the local file to the bucket

print(f'File {local_file_path} uploaded to {bucket_name}/{destination_blob_name}')

# %%
# load data from Cloud Storage to BigQuery

gcs_uri = f"gs://sky_traffic_data/traffic_spreadsheet_{today}.csv"
dataset_id = 'sky_transport_dataset'
table_id = 'sky_transport_table'

bq_client = bigquery.Client() # create a BigQuery client
job_config = bigquery.LoadJobConfig() # configure the job to load data
job_config.source_format = bigquery.SourceFormat.CSV
load_job = bq_client.load_table_from_uri(gcs_uri, dataset_id + '.' + table_id, job_config=job_config) # load data from GCS to BigQuery

print(load_job.result())

# %%
