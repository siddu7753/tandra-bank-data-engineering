\# Tandra Bank — End-to-End Data Engineering Pipeline



\## Project Overview



Tandra Bank is an end-to-end data engineering project that simulates a banking transaction platform and demonstrates how transactional data can be incrementally extracted, validated, transformed, stored in a cloud data lake, processed automatically, and queried for analytics.



The project combines a local operational banking application with an AWS-based data engineering pipeline.



The pipeline processes new banking transactions incrementally from PostgreSQL, performs data quality validation, stores raw and processed datasets in Amazon S3, converts data to Parquet, automatically triggers an AWS Glue workflow through Amazon EventBridge and AWS Lambda, creates a curated daily merchant summary, catalogs the datasets using AWS Glue Data Catalog, and makes the data available for SQL analytics through Amazon Athena.



\---



\## Business Problem



A banking platform continuously generates transaction data. Processing the complete transaction history every time new transactions arrive would be inefficient and increasingly expensive as data volume grows.



The objective of this project is to build an incremental and automated data pipeline that:



\- Processes only newly created transactions.

\- Prevents already processed transactions from being unnecessarily reprocessed.

\- Performs data quality checks before downstream processing.

\- Maintains raw, processed, and curated data lake layers.

\- Stores analytics-ready data efficiently using Parquet.

\- Partitions datasets by processing date.

\- Automatically triggers cloud processing when new processed data arrives.

\- Maintains metadata and schemas through a centralized data catalog.

\- Supports serverless SQL analytics.

\- Handles pipeline failures without incorrectly advancing the processing watermark.



\---



\## High-Level Architecture



The end-to-end data flow is:



Tandra Bank Flask Application  

↓  

PostgreSQL Operational Database  

↓  

Python Incremental Extraction Pipeline  

↓  

Data Quality Validation  

↓  

Amazon S3 Raw Layer (CSV)  

↓  

Python Transformation  

↓  

Parquet Conversion  

↓  

Amazon S3 Processed Layer (Parquet)  

↓  

Amazon EventBridge  

↓  

AWS Lambda  

↓  

AWS Glue Workflow  

↓  

AWS Glue ETL Job  

↓  

Amazon S3 Curated Layer  

↓  

AWS Glue Data Catalog  

↓  

Amazon Athena



\---



\## Core Technologies



| Technology | Purpose |

|---|---|

| Python | Pipeline orchestration, extraction, transformation, validation |

| Flask | Simulated Tandra Bank web application |

| PostgreSQL | Operational transactional database |

| psycopg | Python-to-PostgreSQL connectivity |

| PyArrow | CSV-to-Parquet conversion and schema handling |

| boto3 | Python integration with Amazon S3 |

| Amazon S3 | Raw, processed, and curated data lake storage |

| Amazon EventBridge | Detects processed S3 object creation events |

| AWS Lambda | Starts the AWS Glue workflow |

| AWS Glue | ETL processing, crawling, workflow orchestration, and bookmarks |

| AWS Glue Data Catalog | Central metadata and schema catalog |

| Amazon Athena | Serverless SQL analytics |



\---



\## Data Lake Design



The project uses a single Amazon S3 bucket with separate logical data layers:



`tandra-bank-data`



\### Raw Layer



Raw transaction extracts are stored as CSV files:



`raw/transactions/load\_date=YYYY-MM-DD/`



Example:



`raw/transactions/load\_date=2026-08-30/transactions\_91065\_91084.csv`



\### Processed Layer



Cleaned transactions are converted to Parquet and stored in:



`processed/transactions\_parquet/load\_date=YYYY-MM-DD/`



Example:



`processed/transactions\_parquet/load\_date=2026-08-30/clean\_transactions\_91065\_91084.parquet`



\### Curated Layer



AWS Glue generates analytics-ready daily merchant summaries in:



`curated/merchant\_summary\_daily/`



The curated dataset is used for downstream Athena analytics.





\---



\## Incremental Processing and Watermarking



The pipeline uses incremental extraction rather than reprocessing the entire PostgreSQL transaction table on every run.



A local watermark file stores the last successfully processed transaction ID:



`control/transactions\_watermark.txt`



For example:



`91084`



During the next pipeline run, the extraction process queries PostgreSQL for transactions where:



`transaction\_id > last\_processed\_transaction\_id`



This allows the pipeline to process only newly created transactions.



\### Watermark Safety



The watermark is not updated immediately after extraction.



The pipeline advances the watermark only after:



1\. New transactions are extracted successfully.

2\. Data quality checks pass.

3\. The raw CSV is uploaded to Amazon S3.

4\. Transformation completes successfully.

5\. Parquet conversion completes successfully.

6\. The processed Parquet file is uploaded successfully to Amazon S3.



Only after these steps succeed does the orchestrator update the watermark.



This design prevents transaction loss when a downstream step fails.



For example, during failure testing an invalid S3 bucket caused the processed upload to fail. The watermark remained unchanged. After correcting the S3 destination and rerunning the pipeline, the same transaction batch was safely processed again.



\---



\## Data Quality Validation



Data quality checks are performed during incremental extraction before the transaction batch continues through the pipeline.



The implemented validations include:



\- Duplicate `transaction\_id` detection.

\- NULL `transaction\_id` detection.

\- NULL transaction `amount` detection.



If a validation fails, the extraction process exits with an error and downstream processing does not continue.



This prevents invalid transaction batches from reaching the processed and curated layers.



\---



\## Transformation and Parquet Processing



After a raw transaction batch is successfully extracted, the transformation stage:



\- Reads the newly created raw CSV batch.

\- Preserves the transaction fields.

\- Creates a `transaction\_date` column from `transaction\_time`.

\- Writes a cleaned transaction CSV.

\- Converts the cleaned dataset to Apache Parquet.



The processed Parquet schema includes:



\- `transaction\_id`

\- `account\_id`

\- `merchant\_name`

\- `amount`

\- `transaction\_type`

\- `status`

\- `transaction\_time`

\- `transaction\_date`



The `transaction\_time` field is stored as `timestamp\[ms]` to maintain compatibility with AWS Glue and Amazon Athena.



Parquet was selected for the processed layer because it provides:



\- Columnar storage.

\- Efficient analytical scans.

\- Schema preservation.

\- Reduced storage and query overhead compared with CSV.

\- Native compatibility with AWS Glue and Athena.



\---



\## Partitioning Strategy



Transaction datasets are partitioned using:



`load\_date=YYYY-MM-DD`



Example:



`processed/transactions\_parquet/load\_date=2026-08-30/`



Partitioning allows Athena and AWS Glue to organize transaction batches by processing date and enables partition pruning when queries filter on `load\_date`.



The project maintains the same load-date concept throughout the S3-based pipeline.



\---



\## Event-Driven AWS Automation



The cloud portion of the pipeline is event-driven.



After the local pipeline successfully uploads a processed Parquet file to:



`processed/transactions\_parquet/`



an Amazon EventBridge rule detects the S3 object creation event.



EventBridge invokes an AWS Lambda function.



The Lambda function starts:



`tandra-bank-final-pipeline`



in AWS Glue.



This creates the automated flow:



Processed Parquet uploaded to S3  

↓  

S3 Object Created event  

↓  

Amazon EventBridge  

↓  

AWS Lambda  

↓  

AWS Glue Workflow



No manual Glue workflow execution is required during normal pipeline operation.



\---



\## AWS Glue Processing



The AWS Glue workflow manages the cloud-side processing of the transaction dataset.



The workflow performs the required cataloging and transformation steps and produces the curated merchant-level daily summary.



The processed transaction dataset is cataloged as:



`transactions\_parquet`



The curated analytics dataset is cataloged as:



`merchant\_summary\_daily`



AWS Glue bookmarks are used by the Glue processing design to help manage processing state.



The curated output is written to:



`curated/merchant\_summary\_daily/`



The curated dataset summarizes completed transaction activity for downstream analytics.



\---



\## Failure Handling and Recovery



Failure recovery was explicitly tested as part of the project.



The pipeline is designed so that a failed downstream processed-data upload does not advance the transaction watermark.



Example failure test:



1\. New transactions were created in PostgreSQL.

2\. The pipeline extracted the transactions.

3\. Raw processing completed.

4\. The processed S3 destination was intentionally changed to a nonexistent bucket.

5\. The S3 upload failed.

6\. The watermark remained at the previous successful transaction ID.

7\. The correct S3 destination was restored.

8\. The pipeline was rerun.

9\. The same transaction batch was extracted and processed successfully.

10\. The watermark advanced only after successful processed S3 delivery.



The pipeline also handles:



\- No-new-data runs.

\- Missing or invalid watermark state.

\- PostgreSQL connection/query errors.

\- AWS authentication failures.

\- S3 API failures.

\- Local file-writing failures.



This provides a basic fault-tolerant incremental processing design.



\---



\## No-New-Data Handling



If PostgreSQL contains no transactions newer than the current watermark, the pipeline stops cleanly.



In this situation it does not:



\- Run unnecessary transformations.

\- Create another Parquet batch.

\- Upload processed data.

\- Advance the watermark.

\- Trigger unnecessary downstream AWS processing.



This reduces unnecessary processing and cloud activity.

