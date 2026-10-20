

\# Tandra Bank — Phase 2 AWS Architecture



```mermaid

flowchart TD

&#x20;   A\["Transaction CSV Upload"] --> B\["Amazon S3: Incoming Zone"]

&#x20;   B --> C\["S3 Event Notification"]

&#x20;   C --> D\["Amazon SQS"]

&#x20;   D --> E\["AWS Lambda: Validate and Partition"]

&#x20;   E --> F\["Amazon S3: Raw Zone"]

&#x20;   F --> G\["AWS Glue Data Catalog"]

&#x20;   G --> H\["AWS Glue PySpark ETL"]

&#x20;   H --> I\["Processed Parquet"]

&#x20;   H --> J\["Quarantine Parquet"]

&#x20;   I --> K\["Amazon Athena"]

&#x20;   J --> K

&#x20;   K --> L\["SQL Validation and Reconciliation"]

```



\## Implementation Notes



\- The S3-to-SQS-to-Lambda ingestion stage was implemented in the AWS labs.

\- The Glue historical conflict test was executed using a test CSV and existing processed Parquet data.

\- The main Glue pipeline script was reconstructed and syntax-checked, but has not been independently executed in AWS Glue.

\- The diagram illustrates the intended overall flow, not a verified fully automated end-to-end deployment.



