# API Medallion Data Pipeline

A cloud-native data pipeline that ingests YouTube trending video data through the YouTube Data API v3 and transforms it using the **Medallion Architecture (Bronze → Silver → Gold)** on AWS.

![alt text](<API Medallion Data Pipeline.png>)

The pipeline automates data ingestion, transformation, validation, and analytics using AWS Lambda, Amazon S3, AWS Glue, AWS Step Functions, and Amazon Athena.

## Architecture

```text
                  YouTube Data API v3
                           |
                           v
                 +-------------------+
                 |   AWS Lambda      |
                 |   API Ingestion   |
                 +-------------------+
                           |
                           v
                 +-------------------+
                 |   Bronze Layer    |
                 |   Raw JSON / CSV  |
                 |   Amazon S3       |
                 +-------------------+
                           |
                           v
                 +-------------------+
                 |   Silver Layer    |
                 | Cleaned Parquet   |
                 | AWS Glue / Lambda |
                 +-------------------+
                           |
                           v
                 +-------------------+
                 |  Data Quality     |
                 |  Validation       |
                 +-------------------+
                           |
                     Quality Passed?
                       /       \
                     Yes        No
                      |          |
                      v          v
              +-------------+  +-------------+
              | Gold Layer  |  | SNS Alert   |
              | AWS Glue    |  | Notification|
              +-------------+  +-------------+
                      |
                      v
              +----------------+
              | Amazon Athena  |
              | SQL Analytics  |
              +----------------+
```

AWS Step Functions orchestrates the pipeline, while Amazon EventBridge can trigger scheduled executions. Amazon CloudWatch provides logging and monitoring.

## Overview

This project demonstrates an end-to-end AWS data pipeline that processes live YouTube trending video data and optional historical Kaggle datasets.

The pipeline produces three analytics datasets:

- **Trending Analytics:** Daily video metrics by region.
- **Channel Analytics:** Channel performance, views, and regional rankings.
- **Category Analytics:** Category-level video counts and view-share analysis.

### Key Features

- API-based ingestion using YouTube Data API v3.
- Multi-region data processing across 10 countries.
- Medallion architecture with separate Bronze, Silver, and Gold layers.
- Data transformation using AWS Glue and PySpark.
- JSON-to-Parquet conversion for category reference data.
- Data quality validation before Gold-layer processing.
- Workflow orchestration using AWS Step Functions.
- Automated failure notifications through Amazon SNS.
- Analytics using Amazon Athena and SQL.
- Optional historical data ingestion for backfilling and testing.

## Tech Stack

| Component             | Technology                                 |
| --------------------- | ------------------------------------------ |
| Programming Languages | Python, PySpark, SQL                       |
| Data Source           | YouTube Data API v3                        |
| Historical Data       | Kaggle YouTube Trending Dataset            |
| Compute               | AWS Lambda, AWS Glue                       |
| Storage               | Amazon S3                                  |
| Orchestration         | AWS Step Functions                         |
| Scheduling            | Amazon EventBridge                         |
| Data Catalog          | AWS Glue Data Catalog                      |
| Query Engine          | Amazon Athena                              |
| Notifications         | Amazon SNS                                 |
| Monitoring            | Amazon CloudWatch                          |
| SDK and Libraries     | Boto3, Pandas, AWS SDK for pandas          |
| Data Format           | JSON, CSV, Parquet with Snappy compression |
| Access Management     | AWS IAM                                    |

## Medallion Architecture

### 1. Bronze Layer — Raw Data

The Bronze layer stores source data with minimal transformation to preserve the original input for downstream processing and troubleshooting.

**Data sources:**

- YouTube Data API v3 trending video data.
- YouTube video category reference data.
- Historical Kaggle trending video datasets.

**Processing:**

- Fetch trending videos for configured regions.
- Store raw API responses in Amazon S3.
- Preserve category mappings for reference-data processing.
- Support historical CSV uploads for backfilling and testing.

Example S3 layout:

```text
s3://<bronze-bucket>/
└── youtube/
    ├── raw_statistics/
    │   └── region=IN/
    │       └── date=YYYY-MM-DD/
    │           └── hour=HH/
    └── raw_statistics_reference_data/
        └── region=IN/
```

### 2. Silver Layer — Cleansed Data

The Silver layer standardizes and cleans source data so it can be used reliably for analytics.

**Statistics transformation using AWS Glue and PySpark:**

- Enforce a consistent schema across API and historical data.
- Convert numeric fields to appropriate data types.
- Parse dates and standardize region codes.
- Handle null values and duplicate records.
- Retain the latest record for each video, region, and date.
- Calculate derived metrics such as like ratio and engagement rate.
- Write the transformed data to Amazon S3 in Parquet format.

**Reference-data transformation using AWS Lambda:**

- Normalize YouTube category JSON into tabular records.
- Remove duplicate category entries.
- Convert the normalized data to Parquet using AWS SDK for pandas.
- Partition reference data by region.

Example S3 layout:

```text
s3://<silver-bucket>/
├── statistics/
│   └── region=IN/
└── category_reference/
    └── region=IN/
```

Parquet with Snappy compression improves storage efficiency and supports analytical queries.

### 3. Data Quality Gate

Before the Gold layer runs, a dedicated Lambda function validates the Silver-layer data.

| Validation            | Expected Condition                    |
| --------------------- | ------------------------------------- |
| Minimum row count     | At least 10 rows                      |
| Critical-column nulls | No more than 5%                       |
| Schema validation     | Required columns are present          |
| Value validation      | Views pass sanity checks              |
| Data freshness        | Latest data is less than 48 hours old |

If a validation fails:

1. The pipeline stops before Gold-layer aggregation.
2. An Amazon SNS notification is sent with failure details.
3. CloudWatch logs help investigate the issue.

These thresholds are configurable through Lambda environment variables.

### 4. Gold Layer — Analytics-Ready Data

The Gold layer aggregates validated Silver data into business-oriented datasets using AWS Glue and PySpark.

#### `trending_analytics`

Daily trending metrics grouped by region and date.

| Column                 | Description                   |
| ---------------------- | ----------------------------- |
| `region`               | Country or region code        |
| `trending_date_parsed` | Trending snapshot date        |
| `total_videos`         | Number of trending videos     |
| `total_views`          | Total views                   |
| `total_likes`          | Total likes                   |
| `avg_views_per_video`  | Average views per video       |
| `avg_like_ratio`       | Average like-to-view ratio    |
| `avg_engagement_rate`  | Average engagement rate       |
| `unique_channels`      | Number of distinct channels   |
| `unique_categories`    | Number of distinct categories |

#### `channel_analytics`

Channel-level performance and rankings.

| Column                | Description                            |
| --------------------- | -------------------------------------- |
| `channel_title`       | YouTube channel name                   |
| `region`              | Region code                            |
| `total_videos`        | Number of videos that trended          |
| `total_views`         | Total views across trending videos     |
| `avg_engagement_rate` | Average engagement rate                |
| `times_trending`      | Number of trending appearances         |
| `rank_in_region`      | Channel ranking within the region      |
| `categories`          | Categories associated with the channel |

#### `category_analytics`

Category-level performance and view-share analysis.

| Column                 | Description                      |
| ---------------------- | -------------------------------- |
| `category`             | Video category                   |
| `region`               | Region code                      |
| `trending_date_parsed` | Trending snapshot date           |
| `video_count`          | Number of videos in the category |
| `total_views`          | Total category views             |
| `avg_engagement_rate`  | Average engagement rate          |
| `view_share_pct`       | Percentage share of total views  |

Gold datasets are stored in Parquet format and registered in the AWS Glue Data Catalog for querying through Amazon Athena.

## Pipeline Execution Flow

1. **Ingestion:** Lambda fetches trending video data from the YouTube API and writes raw data to Bronze S3.
2. **Wait:** Step Functions allows a short interval for data consistency.
3. **Silver transformation:** Statistics processing and category reference-data processing run in parallel.
4. **Data quality:** Lambda validates row counts, null percentages, schema, values, and freshness.
5. **Gold aggregation:** AWS Glue generates the analytics datasets if validation succeeds.
6. **Notification:** SNS sends notifications for pipeline success or failure, according to the configured workflow.

Step Functions can implement retries with exponential backoff and error handling for failed tasks.

## Project Structure

```text
API-Medallion-Data-Pipeline/
│
├── lambdas/
│   ├── youtube_api_integstion/
│   │   └── lambda_function.py
│   └── json_to_parquet/
│       └── lambda_function.py
│
├── glue_jobs/
│   ├── bronze_to_silver_statistics.py
│   └── silver_to_gold_analytics.py
│
├── data_quality/
│   └── dq_lambda.py
│
├── step_functions/
│   └── pipeline_orchestation.json
│
├── scripts/
│   ├── aws_copy.sh
│   └── information.md
│
├── data/
│   ├── <region>videos.csv
│   └── <region>_category_id.json
│
├── YouTube Trending Data Pipeline.png
└── README.md
```

_The structure above reflects the intended project layout. Adjust file and directory names to match the files actually present in your repository._

## Prerequisites

Before deploying the pipeline, ensure you have:

- An AWS account with permissions to create and configure the required resources.
- AWS CLI installed and configured.
- Python installed for local development and packaging.
- A YouTube Data API v3 key from Google Cloud Console.
- Appropriate IAM execution roles for Lambda, Glue, Step Functions, and EventBridge.
- S3 buckets for Bronze, Silver, Gold, and deployment scripts.
- AWS Glue databases and an SNS topic for notifications.

## AWS Infrastructure Setup

### 1. Create S3 Buckets

Create separate buckets for each data layer and deployment scripts. S3 bucket names must be globally unique.

```bash
aws s3 mb s3://<bronze-bucket> --region <aws-region>
aws s3 mb s3://<silver-bucket> --region <aws-region>
aws s3 mb s3://<gold-bucket> --region <aws-region>
aws s3 mb s3://<script-bucket> --region <aws-region>
```

Replace the placeholders with your bucket names and AWS Region.

### 2. Create Glue Databases

```bash
aws glue create-database \
  --database-input '{"Name":"yt_pipeline_bronze_dev"}' \
  --region <aws-region>

aws glue create-database \
  --database-input '{"Name":"yt_pipeline_silver_dev"}' \
  --region <aws-region>

aws glue create-database \
  --database-input '{"Name":"yt_pipeline_gold_dev"}' \
  --region <aws-region>
```

### 3. Create an SNS Topic

```bash
aws sns create-topic \
  --name yt-data-pipeline-alerts-dev \
  --region <aws-region>
```

Subscribe an email address to the topic and confirm the subscription:

```bash
aws sns subscribe \
  --topic-arn <sns-topic-arn> \
  --protocol email \
  --notification-endpoint <your-email>
```

## Configuration

### Ingestion Lambda

Configure the following environment variables:

| Variable           | Description                  |
| ------------------ | ---------------------------- |
| `YOUTUBE_API_KEY`  | YouTube Data API v3 key      |
| `S3_BUCKET_BRONZE` | Bronze S3 bucket name        |
| `YOUTUBE_REGIONS`  | Comma-separated region codes |

Example region configuration:

```text
US,GB,CA,DE,FR,IN,JP,KR,MX,RU
```

**Security:** Do not hardcode API keys or commit them to Git. Use a suitable secrets-management approach and restrict access through IAM.

### Data Quality Lambda

| Variable              | Description                       |
| --------------------- | --------------------------------- |
| `S3_BUCKET_SILVER`    | Silver S3 bucket name             |
| `GLUE_DB_SILVER`      | Silver Glue database              |
| `SNS_ALERT_TOPIC_ARN` | SNS topic ARN                     |
| `DQ_MIN_ROW_COUNT`    | Minimum acceptable row count      |
| `DQ_MAX_NULL_PERCENT` | Maximum permitted null percentage |

Example values:

```text
DQ_MIN_ROW_COUNT=10
DQ_MAX_NULL_PERCENT=5.0
```

### Glue Job Parameters

Pass the required parameters through Step Functions or the Glue job configuration.

| Parameter           | Description          |
| ------------------- | -------------------- |
| `--bronze_database` | Bronze Glue database |
| `--bronze_table`    | Bronze source table  |
| `--silver_database` | Silver Glue database |
| `--silver_bucket`   | Silver output bucket |
| `--gold_database`   | Gold Glue database   |
| `--gold_bucket`     | Gold output bucket   |

Use the exact argument names expected by your Glue scripts.

## Deployment

### 1. Upload Glue Scripts

```bash
aws s3 cp glue_jobs/bronze_to_silver_statistics.py \
  s3://<script-bucket>/glue_jobs/

aws s3 cp glue_jobs/silver_to_gold_analytics.py \
  s3://<script-bucket>/glue_jobs/
```

### 2. Deploy Lambda Functions

Package each Lambda function with its required dependencies.

For example, from the ingestion Lambda directory:

```bash
cd lambdas/youtube_api_integstion
zip -r function.zip lambda_function.py
```

Deploy the package using the AWS Console or AWS CLI. Configure the function's runtime, handler, execution role, timeout, memory, and environment variables.

**Dependency note:** If a Lambda imports `awswrangler`, ensure the compatible AWS SDK for pandas Lambda layer or packaged dependencies are configured for your Python runtime, AWS Region, and CPU architecture.

### 3. Create AWS Glue Jobs

Create Glue jobs that reference the uploaded scripts and an appropriate Glue execution role. Configure the Glue version, worker type, number of workers, job parameters, and required permissions.

### 4. Deploy Step Functions

Use the state machine definition in your repository:

```bash
aws stepfunctions create-state-machine \
  --name yt-data-pipeline \
  --definition file://step_functions/pipeline_orchestation.json \
  --role-arn <step-functions-role-arn> \
  --region <aws-region>
```

If the state machine already exists, update its definition instead of creating a duplicate.

### 5. Configure Scheduled Execution

Create an EventBridge schedule to trigger the state machine at your preferred interval. Ensure the EventBridge execution role has permission to start the state machine.

For historical data, upload the CSV and reference JSON files to the appropriate Bronze S3 prefixes using the provided script or AWS CLI.

## Running the Pipeline

### Manual Execution

Start the Step Functions execution:

```bash
aws stepfunctions start-execution \
  --state-machine-arn <state-machine-arn> \
  --region <aws-region>
```

### Scheduled Execution

Configure EventBridge to run the workflow on a schedule, such as every six hours. Choose an interval that fits the YouTube API quota, data freshness requirements, and AWS cost budget.

## Monitoring and Troubleshooting

| AWS Service        | Purpose                                                |
| ------------------ | ------------------------------------------------------ |
| Step Functions     | Inspect workflow execution history and failed states   |
| CloudWatch Logs    | Review Lambda and Glue logs                            |
| CloudWatch Metrics | Monitor execution errors, duration, and resource usage |
| Amazon SNS         | Receive configured success and failure notifications   |
| Amazon Athena      | Query datasets and validate analytics results          |

Common issues to investigate:

- **Lambda dependency errors:** Check the deployment package and compatible layers.
- **S3 access denied:** Verify IAM permissions, bucket policies, and encryption permissions.
- **Glue job failures:** Review script arguments, input schemas, catalog tables, and CloudWatch logs.
- **Data quality failures:** Inspect row counts, null percentages, schema changes, and freshness.
- **API errors or quota limits:** Verify the API key, enabled YouTube Data API v3, request quota, and region configuration.
- **Step Functions failures:** Inspect the failed state and confirm retry, timeout, and error-handling configuration.

## Example Athena Queries

### Top Trending Channels in India

```sql
SELECT
    channel_title,
    total_views,
    times_trending,
    rank_in_region
FROM yt_pipeline_gold_dev.channel_analytics
WHERE region = 'IN'
ORDER BY total_views DESC
LIMIT 10;
```

### Trending Metrics by Region

```sql
SELECT
    region,
    SUM(total_videos) AS total_videos,
    SUM(total_views) AS total_views,
    AVG(avg_engagement_rate) AS avg_engagement_rate
FROM yt_pipeline_gold_dev.trending_analytics
GROUP BY region
ORDER BY total_views DESC;
```

### Most Viewed Categories

```sql
SELECT
    category,
    region,
    SUM(total_views) AS total_views,
    AVG(view_share_pct) AS avg_view_share_pct
FROM yt_pipeline_gold_dev.category_analytics
GROUP BY category, region
ORDER BY total_views DESC
LIMIT 20;
```

These examples assume the Glue Data Catalog database and table names match the configuration used during deployment.

## Supported Regions

| Code | Country        |
| ---- | -------------- |
| US   | United States  |
| GB   | United Kingdom |
| CA   | Canada         |
| DE   | Germany        |
| FR   | France         |
| IN   | India          |
| JP   | Japan          |
| KR   | South Korea    |
| MX   | Mexico         |
| RU   | Russia         |

## Data Sources

- **YouTube Data API v3:** Primary source for live trending video data and category reference information.
- **Kaggle YouTube Trending Dataset:** Optional historical source for backfilling, testing, and processing historical CSV data.

## Security and Cost Considerations

- Store API keys securely; never commit credentials to Git.
- Follow least-privilege IAM principles for Lambda, Glue, Step Functions, and S3.
- Enable appropriate S3 encryption and block public access.
- Configure CloudWatch log retention to manage storage costs.
- Monitor YouTube API quota usage and AWS service costs.
- Choose Glue worker capacity and pipeline schedules based on actual data volume.
- Consider S3 lifecycle policies for historical raw data and intermediate outputs.

## Future Improvements

- Infrastructure as Code using Terraform or AWS CloudFormation.
- Automated testing and CI/CD deployment.
- Schema evolution and stronger data contracts.
- Incremental processing and idempotent pipeline execution.
- Cost optimization for Glue jobs and S3 storage.
- Dashboard development for trending, channel, and category analytics.
- Expanded data observability and pipeline metrics.

## License

Choose a license for this repository before distributing or reusing the project. If no license is specified, the repository does not automatically grant others permission to reuse its contents.
