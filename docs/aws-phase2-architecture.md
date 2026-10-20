

\# Tandra Bank — Phase 2 AWS Architecture



```mermaid

flowchart TD

&#x20;   A\["Transaction CSV Upload"]

&#x20;   B\["Amazon S3 - Incoming Zone"]

&#x20;   C\["S3 Event Notification"]

&#x20;   D\["Amazon SQS"]

&#x20;   E\["AWS Lambda - Validate and Partition"]

&#x20;   F\["Amazon S3 - Raw Zone"]

&#x20;   G\["AWS Glue Data Catalog"]

&#x20;   H\["AWS Glue PySpark ETL"]

&#x20;   I\["Processed Parquet"]

&#x20;   J\["Quarantine Parquet"]

&#x20;   K\["Amazon Athena"]

&#x20;   L\["SQL Validation and Reconciliation"]



&#x20;   A --> B

&#x20;   B --> C

&#x20;   C --> D

&#x20;   D --> E

&#x20;   E --> F

&#x20;   F --> G

&#x20;   G --> H

&#x20;   H --> I

&#x20;   H --> J

&#x20;   I --> K

&#x20;   J --> K

&#x20;   K --> L

```



\## Implementation Notes



\- The S3-to-SQS-to-Lambda ingestion stage was implemented in the AWS labs.

\- The Glue historical conflict test was executed using a test CSV and existing processed Parquet data.

\- The main Glue pipeline script was reconstructed and syntax-checked but has not been independently executed in AWS Glue.

\- The diagram represents the intended overall architecture, not a verified fully automated end-to-end deployment.



