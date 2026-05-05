# NYC Taxi Data Pipeline

A real-time data engineering pipeline that ingests NYC Yellow Taxi trip data, streams it through AWS Kinesis, stores it in S3, transforms it with AWS Glue and dbt, and exposes analytical mart tables via Amazon Athena.

---

## Architecture Overview

```
Local Parquet Files
       |
       v
  producer.py          (Python — streams ~20 records/sec)
       |
       v
AWS Kinesis Data Streams  (stream: nyc-taxi-stream)
       |
       v
AWS Kinesis Firehose      (buffers 5 MB or 60s, partitioned by year/month)
       |
       v
Amazon S3                 (s3://nyc-taxi-de-dsalina/raw/...)
       |
       v
AWS Glue Crawler          (catalogs raw data → nyc_taxi_db.raw)
       |
       v
AWS Glue ETL Job          (cleans, enriches, writes curated Parquet)
       |
       v
Amazon S3                 (s3://nyc-taxi-de-dsalina/curated/trips/ — partitioned by pickup_date)
       |
       v
AWS Glue Data Catalog     (nyc_taxi_db.trips — registered for Athena)
       |
       v
dbt (Athena adapter)      (staging views → analytical mart tables)
       |
       v
Amazon Athena             (mart_hourly_demand, mart_top_zones, payment_summary)
```

---

## Repository Structure

```
DE_TAXI_PROJECT/
├── data/                          # Local source files (gitignored)
│   ├── yellow_tripdata_2023-01.parquet
│   └── yellow_tripdata_2023-02.parquet
├── src/
│   ├── producer/
│   │   └── producer.py            # Kinesis stream producer
│   └── glue/
│       └── etl_job.py             # AWS Glue PySpark ETL job
├── taxi_transforms/               # dbt project (Athena adapter)
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/
│       │   ├── schema.yml         # Source definition (nyc_taxi_db.trips)
│       │   └── stg_taxi_trips.sql # Cleaned/cast staging view
│       └── marts/
│           ├── mart_hourly_demand.sql  # Hourly trip demand + revenue
│           ├── mart_top_zones.sql      # Top pickup zones by volume
│           └── payment_summary.sql     # Payment type breakdown
├── infra/
│   ├── firehose-config.json       # Kinesis Firehose S3 delivery config
│   ├── trust-policy.json          # IAM trust policy for FirehoseDeliveryRole
│   └── permission-policy.json     # IAM permission policy (S3 + CloudWatch Logs)
└── .gitignore
```

---

## Components

### 1. Producer (`src/producer/producer.py`)

Reads NYC Yellow Taxi Parquet files locally and publishes each trip record as a JSON event to the Kinesis Data Stream `nyc-taxi-stream`.

- Adds a unique `ride_id` (UUID) and `ingestion_timestamp` to each record
- Partitions by `PULocationID` for even shard distribution
- Throttles at ~20 events/sec (50 ms sleep)
- Targets AWS region `us-east-1`

**Run:**
```bash
pip install boto3 pandas pyarrow
python src/producer/producer.py
```

> Requires AWS credentials with `kinesis:PutRecord` permission on `nyc-taxi-stream`.

---

### 2. Kinesis Firehose (`infra/firehose-config.json`)

Buffers records from the Kinesis stream and delivers them to S3.

| Setting | Value |
|---|---|
| Destination bucket | `s3://nyc-taxi-de-dsalina` |
| S3 prefix | `raw/year=!{timestamp:yyyy}/month=!{timestamp:MM}/` |
| Error prefix | `raw-errors/!{firehose:error-output-type}/year=.../month=.../` |
| Buffer size | 5 MB |
| Buffer interval | 60 seconds |
| IAM role | `FirehoseDeliveryRole` |

---

### 3. AWS Glue ETL (`src/glue/etl_job.py`)

PySpark job that reads raw data from the Glue Data Catalog, cleans it, enriches it, and writes curated Parquet back to S3.

**Transformations applied:**
- Filters out records where `fare_amount <= 0`, `trip_distance <= 0`, or dropoff is before pickup
- Adds `trip_duration_mins` (derived from pickup/dropoff timestamps)
- Adds `fare_per_mile` (fare amount divided by trip distance)
- Adds `pickup_date` and `pickup_hour` columns

**Output:** `s3://nyc-taxi-de-dsalina/curated/trips/` — Parquet, partitioned by `pickup_date`, overwrite mode.

**Deploy:** Upload `etl_job.py` to `s3://nyc-taxi-de-dsalina/scripts/` and reference it when creating the Glue job.

---

### 4. dbt Transforms (`taxi_transforms/`)

dbt project targeting Amazon Athena via the `dbt-athena-community` adapter.

**Source:** `awsdatacatalog.nyc_taxi_db.trips` (the curated table registered by Glue Crawler)

#### Staging (materialized as views)

| Model | Description |
|---|---|
| `stg_taxi_trips` | Casts raw columns to correct types, renames fields, filters invalid rows |

#### Marts (materialized as tables)

| Model | Description |
|---|---|
| `mart_hourly_demand` | Trip count, revenue, tips, and avg fare/distance grouped by hour; includes weekend flag |
| `mart_top_zones` | Top pickup zones ranked by trip volume with avg fare and tip % |
| `payment_summary` | Breakdown by payment type (Credit Card, Cash, etc.) with % share of total trips |

**Run dbt:**
```bash
cd taxi_transforms
dbt run
dbt test
dbt docs generate && dbt docs serve
```

---

### 5. IAM Infrastructure (`infra/`)

| File | Purpose |
|---|---|
| `trust-policy.json` | Allows `firehose.amazonaws.com` to assume `FirehoseDeliveryRole` |
| `permission-policy.json` | Grants the role `s3:PutObject` (and related) on the delivery bucket and `logs:PutLogEvents` |

---

## Data Source

NYC Taxi & Limousine Commission (TLC) Yellow Taxi Trip Records — January and February 2023.

**Schema columns (19 fields):**
`VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `RatecodeID`, `store_and_fwd_flag`, `PULocationID`, `DOLocationID`, `payment_type`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `total_amount`, `congestion_surcharge`, `airport_fee`

Source data files are excluded from version control (`.gitignore`). Download from the [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) page.

---

## Prerequisites

| Tool | Version |
|---|---|
| Python | 3.9+ |
| boto3 | latest |
| pandas + pyarrow | latest |
| AWS CLI | v2 |
| dbt-athena-community | latest |

AWS services required: Kinesis Data Streams, Kinesis Firehose, S3, Glue (Crawler + ETL), Athena, IAM.

---

## Setup

1. **Configure AWS credentials:**
   ```bash
   aws configure
   ```

2. **Create the Kinesis stream:**
   ```bash
   aws kinesis create-stream --stream-name nyc-taxi-stream --shard-count 2 --region us-east-1
   ```

3. **Create the Firehose delivery stream** using `infra/firehose-config.json` as the S3 destination config.

4. **Create the IAM role** `FirehoseDeliveryRole` using `infra/trust-policy.json` and attach `infra/permission-policy.json`.

5. **Run the producer** to stream data into Kinesis.

6. **Run the Glue Crawler** on `s3://nyc-taxi-de-dsalina/raw/` to register `nyc_taxi_db.raw` in the catalog.

7. **Run the Glue ETL job** (`etl_job.py`) to produce the curated dataset.

8. **Run the Glue Crawler** again on `s3://nyc-taxi-de-dsalina/curated/trips/` to register `nyc_taxi_db.trips`.

9. **Run dbt** to build staging views and mart tables in Athena.

---

## Author

Dennis Salinas
