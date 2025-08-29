# Wistia Video Analytics Pipeline

This project implements an **end-to-end AWS-based pipeline** to ingest, transform, and store Wistia video analytics for reporting and downstream consumption.
The solution uses **AWS Lambda**, **AWS Glue**, **Amazon Athena**, and **AWS Step Functions** to orchestrate ingestion, transformations, and loading into a dimensional model.

## Infrastructure Set up (Micro Services and Service Enablement)

The pipeline is built using AWS-managed services for scalability, cost efficiency, and reduced operational overhead.

**Enabled and configured services:**

* **AWS Lambda** – Used for data ingestion from Wistia API (Media and Events).
* **AWS Glue Jobs** – Pyspark ETL for transforming raw JSON data into partitioned Parquet datasets.
* **AWS Glue Crawlers** – Automatic schema detection and table creation in AWS Glue Data Catalog.
* **Amazon S3** – Centralized storage for raw and processed datasets.
* **AWS Step Functions** – Orchestration of the full workflow from ingestion to transformation and catalog updates.
* **AWS EventBridge** - Schedule the Step functions or individual services for batch ingestion, transformation and data table availability.
* **Amazon Athena** – Serverless SQL queries for processed datasets.

## Dimensional Model

While exploring different API endpoints, the events API endpoint had a lot of data useful for the media engagament table, so, it was chosen instead of the visitors endpoint.

![Dimensional Model](docs/dimensional-data-model-schema.png)

## Architecture Diagram

![Architecture Diagram](docs/ak-wistia-video-analytics-architecture.png)

```mermaid
flowchart TD
    A[Wistia API] -->|Media JSON| L1[Lambda: Ingest Media]
    L1 --> G1[Glue Job: Transform dim_media]
    G1 --> C1[Glue Crawler: dim_media table]

    A2[Wistia API] -->|Events JSON| L2[Lambda: Ingest Events]
    L2 --> G2[Glue Job: Transform fact_events]
    G2 --> C2[Glue Crawler: fact_events table]

    G1 & G2 --> G3[Glue Job: Transform fact_media_engagement]
    G3 --> C3[Glue Crawler: fact_media_engagement table]

    subgraph Step Functions
        L1 --> G1 --> C1 --> L2 --> G2 --> C2 --> G3 --> C3
    end

    Athena[(Athena Queries)] --> R[(Downstream Dashboards)]
```
---
## Observations

* **Lambda cold start deployment issues** were resolved by:

  * Packaging dependencies in a `.zip` and uploading to S3 for Lambda layers.
  * Setting correct **IAM execution role permissions** for S3 read/write and Wistia API access.

* The **incremental events ingestion** Lambda now:

  * Pulls only events for the last 24 hours.
  * Skips S3 upload if there are **no new events**.
  * Maintains a `latest_event_file.json` pointer file to track the most recent uploaded file.

* **Glue ETL jobs** were modified to:

  * Read only the **latest file** from the raw S3 path.
  * Avoid creating empty partitions when no new data exists.

* **Step Functions** successfully orchestrate the pipeline:

  * Media ingestion → Media dimension transform → Events ingestion → Events fact transform → Media engagement fact transform.
  * Added **Glue Crawlers** after each transform job for schema refresh.

* **EventBridge** schedules the state machine:
  
  * We **ran the pipeline once** manually and **scheduled it** with EventBridge for recurring runs.
  * Made sure the events are captured for the entire UTC day from 00:00 to 23:59.

---

## Assumptions

* Wistia API token is stored securely in **AWS Lambda environment variables**.
* S3 bucket structure is consistent:

  ```
  raw_data/media/
  raw_data/events/
  processed/dim_media/
  processed/fact_events/
  processed/fact_media_engagement/
  ```

* Media and events are joined on a common `media_id` key for the engagement fact table.
* Athena tables are created by Glue Crawlers with correct schema inference from processed parquet files.
* The **list of media IDs** to pull events for is predefined and controlled via Lambda environment variables.

---

## Key Steps

1. **Ingest Media Metadata**

   * Lambda fetches all media records from Wistia API.
   * Uploads raw JSON to S3 under `raw_data/media/`.

2. **Transform dim\_media**

   * AWS Glue job flattens JSON, selects required fields, and writes parquet to `processed/dim_media/`.
   * Glue Crawler updates Athena table.

3. **Ingest Events Data**

   * Lambda fetches events for each target media ID for the last 24 hours.
   * Uploads to `raw_data/events/` only if events exist.
   * Updates `latest_event_file.json`.

4. **Transform fact\_events**

   * AWS Glue job reads **only the latest file** from `raw_data/events/`.
   * Writes parquet to `processed/fact_events/`.
   * Glue Crawler updates Athena table.

5. **Transform fact\_media\_engagement**

   * AWS Glue job joins `dim_media` and `fact_events`.
   * Calculates engagement metrics (e.g., total watch time, average % viewed).
   * Writes parquet to `processed/fact_media_engagement/`.
   * Glue Crawler updates Athena table.

6. **Pipeline Orchestration**

   * AWS Step Functions sequence:

     ```
     Ingest Media → Transform dim_media → Crawler
     → Ingest Events → Transform fact_events → Crawler
     → Transform fact_media_engagement → Crawler
     ```
   * Retry and failure handling per step.

---

## 7-Day Run

The pipeline was run using a Step Function that was scheduled using Amazon EventBridge at 23:50:00 UTC so that all the events occurred 
after 00:00:00 UTC the same day will be extracted by the Ingest Events Data lambda function.

Implemented **CloudWatch metrics** and alarms for ingestion successes, inovations by creating a custom dashboard: **WistiaPipelineHealth**. Some of the automatic dashboards that AWS provides are used to get to the custom dashboard.

### Below is an overview of the runs:

![Metrics 7-day run](docs/metrics-7-day-run-wistia.png)

As seen above s3 object trend, one new object is added correponding to the raw data folder of `dim_media` table giving the latest snapshot per each new run.

### Cost overview:

![Cost overview](docs/cost-breakdown-wistia.png)

Running glue job is the only thing that is not free in using AWS Free Tier.

## Downstream Consumption

* **Athena** provides a query interface for analysts to explore:

  * Media details
  * Event-level interactions
  * Engagement summaries

* Data is ready for visualization in:

  * **Amazon QuickSight**
  * BI tools like Tableau, Power BI

* Possible future integration into:

  * Marketing analytics dashboards
  * Automated performance alerts

## Alternatives to using Athena + Quicksight: Delta Lake. Is it suitable?

Delta like has a lot of other features, like time travel, faster querying, faster writing, acid compliance. In this project's context, delta lake would be overkill for a few reasons:

* Only one table (fact_events) is very dynamic and new data is streaming in only intermittently. So, having the latest state table for `fact_media_engagement` would be good as long as new data is added at the source. 
* `dim_media` is very static so I made an assumption that I can apply SCD 1. A simpler way of handling version log of this table is to have different version of datasets based on the run date. And it has been implemented by storing the .json file with media-id data.
* `fact_media_engagement` is built based on fact_events so if fact_events doesn’t have any new data, fact_media_engagement won’t have any changes except for when it is loaded.

## Possible Next Steps

* Parallelize the data ingestion and subsequent transformation for different API endpoints.
* Add **parameterized Step Functions input** to support ad-hoc date range runs.
* Extend the pipeline for **Wistia audience analytics** and **geo-level insights**.



