## Overview

OmniRoute is a data engineering project that processes batch and real-time logistics data to monitor fleet operations, driver safety, and fuel efficiency.

It combines:

- Batch data from AWS S3 (vehicle registry, assignments, fuel logs)
- Real-time telemetry from Kafka (GPS, speed, sensor data)

The system generates insights such as:

- Driver safety violations
- Fuel efficiency anomalies
- Vehicle assignment history (SCD Type 2)

## Objectives

- Track vehicle-driver history using SCD Type 2
- Detect abnormal fuel consumption (>12% deviation)
- Monitor real-time safety violations (speeding, restricted zones)
- Apply penalty system based on safety strikes
- Generate daily and monthly reports

## Setup Instructions

1. Clone the repository:

```bash
git clone https://github.com/Saint-Potato/omniroute
```

2. Create virtual environment: 

```bash
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies: 

```bash
pip install -r requirements.txt
```

## How to Run

- Run batch pipelines using Spark:

```bash

```

- Start streaming job:

```bash

```

- Run airflow DAG:

```bash
airflow scheduler
airflow webserver
```