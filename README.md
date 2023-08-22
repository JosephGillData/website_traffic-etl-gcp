# Sky Task 1

### Introduction

This repository automates an ETL (extract, transform, load) solution using Google Cloud in which an excel file is backed up with Google Cloud Storage and then inserted into a BigQuery database each day.

### Task

A spreadsheet, traffic_spreadsheet.xls, needs to be uploaded to BigQuery for analysis, such that:
* The file needs to be converted to .csv as BigQuery does not support .xls.
* The time column is of dd/mm/YY HH:MM format which is not natively supported in BigQuery. The time column needs to be reformatted to YY-mm-dd HH:MM before it can be uploaded.
* We require a backup of the file in an S3 storage bucket in GCS.
* The data needs to be uploaded to BigQuery from GCS.
* Create a dashboard on Data Studio should be created showing a graph of traffic over time.
* The whole process needs to be automated to run daily via a cronjob.

### Getting Started

1. Clone this repository.
2. Create a `.env` file with 
```
GOOGLE_APPLICATION_CREDENTIALS=
GITHUB_CLIENT_SECRET=
GITHUB_CLIENT_ID=
```
3. Create a Google Cloud `canvas-radio-396115-06ab17b6af40.json` file containing your Google service account credentials.
4. Navigate to this folder in the terminal and crate a virtual environment - `python3 -m venv venv`.
5. Activate the virtual environment - `source venv/bin/activate`.
6. Install the required packages - `pip install -r requirements.txt`.
7. Create a cron job with `crontab -e` and `0 0 * * *` to set at midnight.

### The Dataset

The data is stored in `data/traffic_spreadsheet.xls`. This dataset contains information on the network traffic on the 23rd May 2021. Each row represents a network data point. There are 2 columns and 287 rows. Here's an explanation of each column:
* **time**: The date and time when the observation occurred.
* **traffic**: The traffic volume associated with each observation, probably measured in gigabits per second (Gbps). 

### ETL Process

The ETL process is performed in the `task-1.py` Python script.

#### Extract

* The `traffic_spreadsheet.xls` spreadsheet is loaded and converted to a dataset using pandas. 

#### Tranform

* The time column is reformated to a suitable format for Google Big Query.
* I add a created_at column so we can track the date that each row was processed.
* The dataset is saved as a `.csv` file.

#### Load

* Create a backup of the original file in an S3 storage bucket in Google Cloud Storage (GCS). This ensures data redundancy and preservation of the source data.
* Load the data from GCS into BigQuery.

### Assumptions

* I added a created_at column for each row, so that we can monitor when data was created in the BiqQuery database.
* I appened the date and time to each traffic_spreadsheet file so that we have a backup of each day's data.
* I binned the data per hour to show the daily trend.

### Further Steps

* Deploy the ETL process the the cloud, rather than being run locally.
* Connect to a live data source, eather than loading a static .csv file. This is more useful.
* Automate the `traffic_per_hour_query` to create the `traffic_per_hour` table.
* Improve the styling of the Data Stutdio dashbaord.

### Author 

Joseph Gill 

- [Visit My Personal Website](https://joegilldata.com)
- [LinkedIn Profile](https://www.linkedin.com/in/joseph-gill-726b52182/)
- [Twitter Profile](https://twitter.com/JoeGillData)
