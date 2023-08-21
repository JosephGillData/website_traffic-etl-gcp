# `Sky Task 1`

### Introduction

This repository automates an ETL (extract, transform, load) solution using Apache Airflow in which an email is sent containing the top 3 IP addresses with the most amount of traffic in the AM and PM each day.

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

The data is stored in `data/traffic_data.csv`. This dataset contains information on the network traffic of a site on the 13th August 2021. Each row represents a network data point or record. There are over 5 columns and over 60,000 rows. Here's an explanation of each column:
* **bf_date**: The date when the observation occurred.
* **bf_time**: The time when the observation occurred.
* **id**: A unique identifier for each observation.
* **ip**: The IP address associated with each observation.
* **gbps**: The traffic volume associated with each observation, measured in gigabits per second (Gbps). 

### ETL Process

The ETL process is performed by a directed acyclical graph (DAG) created in the `task-3` Python script. The image below shows the tasks that form the DAG and how they intereact via task dependencies.

![Alt Text](Dag.png)

#### Extract

* `read_traffic_data`: loads data from `traffic_data.csv` and creates a pandas dataset.

#### Tranform
* `filter_ips`: filters out 20% of the IP addresses with the lowest traffic.
* `split_am_pm`: creates a branch for the AM and PM data to be analysed in parallel.
* `filter_am`: filters the data to obtain observations created before midday.
* `filter_pm`: filters the data to obtain observations after before midday.

#### Load
* `day_of_week`: triggers the `do_nothing_am` function if it's a weekday and triggers the `send_email_am` if it's a weekend.
* `do_nothing_am`: does nothing.
* `send_email_am`: obtains the three IP addresses with the most traffic before midday and sends an email containing them.
* `send_email_pm`: obtains the three IP addresses with the most traffic after midday and sends an email containing them.

### Author 

Joseph Gill 

- [Visit My Personal Website](https://joegilldata.com)
- [LinkedIn Profile](https://www.linkedin.com/in/joseph-gill-726b52182/)
- [Twitter Profile](https://twitter.com/JoeGillData)
