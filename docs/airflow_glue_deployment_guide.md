# Deployment Guide: Airflow (EC2) & AWS Glue

This guide covers the end-to-end steps required to deploy the OmniRoute Bronze ingestion pipeline, which involves an AWS Glue job orchestrated by Apache Airflow running on an EC2 instance.

## Phase 1: AWS Setup (Glue & S3)

### 1. Upload the Glue Script to S3
AWS Glue requires the Python script to be stored in S3 before creating the job.
Run this from your local machine:
```bash
aws s3 cp glue_jobs/daily_ingest_bronze_glue.py \
    s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/glue-scripts/daily_ingest_bronze_glue.py
```

### 2. Create the Glue Job (AWS Console)
1. Go to the **AWS Glue Console** > **Data Integration and ETL** > **Jobs**.
2. Select **Script editor** -> **Spark** -> **Upload and edit an existing script** (or just create a new job and point it to the S3 path above).
3. Configure the job with these **exact** settings (matching your `s3_paths.json`):
   - **Name**: `omniroute-daily-ingest-bronze`
   - **IAM Role**: `omniroute-glue-role` (Ensure this role has S3 Read/Write access to the bronze bucket).
   - **Type**: `Spark`
   - **Glue Version**: `Glue 4.0`
4. Under **Job Details** > **Advanced properties**:
   - **Worker type**: `G.1X`
   - **Requested number of workers**: `2`
   - **Job timeout**: `30`
   - **Maximum concurrency**: `1`
   - *Note: Leave "Job parameters" empty. Airflow will pass parameters (`--run_date`, etc.) dynamically at runtime.*
5. Save the job.

---

## Phase 2: Airflow Environment Setup (EC2)

### 1. Install AWS Provider on EC2
Your Airflow environment needs the Amazon provider package to interact with AWS Glue.
SSH into your EC2 server and run:
```bash
# Activate your airflow virtual environment first (if you have one)
pip install apache-airflow-providers-amazon

# Restart the Airflow scheduler and webserver to load the new provider
sudo systemctl restart airflow-scheduler
sudo systemctl restart airflow-webserver
```

### 2. Configure AWS Connection in Airflow
Airflow needs credentials to trigger the Glue job.
1. Open the **Airflow Web UI** (e.g., `http://<EC2-IP>:8080`).
2. Go to **Admin** > **Connections**.
3. Create a new connection:
   - **Connection Id**: `aws_default`
   - **Connection Type**: `Amazon Web Services`
   - **AWS Access Key ID**: (Your IAM user access key)
   - **AWS Secret Access Key**: (Your IAM user secret key)
   - **Extra**: `{"region_name": "us-east-1"}`
   
   *(Note: If your EC2 instance itself has an IAM Instance Profile attached with Glue permissions, you can leave the keys blank and Airflow will assume the EC2 role automatically).*

---

## Phase 3: Deploy Code to EC2

### 1. Understand the Directory Structure
The DAG (`omniroute_bronze_glue_dag.py`) is programmed to look for `s3_paths.json` exactly **one directory above** itself:
```text
<AIRFLOW_HOME>/
├── s3_paths.json                 <-- MUST be here
└── dags/
    └── omniroute_bronze_glue_dag.py
```
*(Assuming your `AIRFLOW_HOME` is `~/airflow` or `/opt/airflow`)*

### 2. Copy Files to EC2
From your local machine, use `scp` to copy the files to the correct locations on your EC2 instance. Replace `~/airflow/` with your actual Airflow home directory path.

```bash
# Upload s3_paths.json to the parent directory of the dags folder
scp -i ~/.ssh/your-key.pem s3_paths.json ubuntu@<EC2-IP>:~/airflow/s3_paths.json

# Upload the DAG to the dags folder
scp -i ~/.ssh/your-key.pem dags/omniroute_bronze_glue_dag.py ubuntu@<EC2-IP>:~/airflow/dags/omniroute_bronze_glue_dag.py
```

---

## Phase 4: Run & Monitor

1. Open the **Airflow Web UI**.
2. Refresh the DAGs page. You should see `omniroute_bronze_glue_ingest` appear.
3. Unpause the DAG (click the toggle switch next to the DAG name).
4. Click the **Trigger DAG** (Play button) > **Trigger DAG**.
5. Click on the DAG run and inspect the `trigger_glue_bronze_ingest` task.
6. Check the **Task Logs** in Airflow. You will see that Airflow submits the job, obtains a `JobRunId`, and continuously polls AWS until the job reaches `SUCCEEDED`.
7. You can verify the run simultaneously in the **AWS Glue Console** under **Job runs**. You should see the custom parameters (like `--run_date`) were successfully passed.
