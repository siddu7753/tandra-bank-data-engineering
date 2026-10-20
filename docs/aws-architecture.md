\# Tandra Bank — AWS Data Engineering Architecture



\## 1. Project Overview



Tandra Bank is a hands-on banking data engineering project developed in two phases:



\- \*\*Phase 1 — Local Data Engineering:\*\* PostgreSQL, Python, Flask, CSV processing, and local ETL pipelines.

\- \*\*Phase 2 — AWS Data Engineering:\*\* Amazon S3, IAM, SQS, Lambda, AWS Glue, PySpark, Glue Data Catalog, and Amazon Athena.



The project demonstrates transaction ingestion, data transformation, incremental processing, data quality validation, historical conflict detection, and reconciliation.



\## 2. Architecture



```text

Banking Transaction CSV

&#x20;        |

&#x20;        v

Amazon S3 — Incoming Zone

&#x20;        |

&#x20;        v

S3 Event Notification

&#x20;        |

&#x20;        v

Amazon SQS

&#x20;        |

&#x20;        v

AWS Lambda Transaction Processor

&#x20;        |

&#x20;        v

Amazon S3 — Raw Zone

(year/month/day partitions)

&#x20;        |

&#x20;        v

AWS Glue Data Catalog

&#x20;        |

&#x20;        v

AWS Glue PySpark ETL

&#x20;        |

&#x20;        +----------------------+

&#x20;        |                      |

&#x20;        v                      v

Processed Parquet         Quarantine Parquet

&#x20;        |                      |

&#x20;        +-----------+----------+

&#x20;                    |

&#x20;                    v

&#x20;             Amazon Athena

&#x20;                    |

&#x20;                    v

&#x20;          SQL Validation \& Analytics

```



This represents the overall data flow. The ingestion and Glue processing stages were exercised in hands-on labs; the final Lab 30 historical conflict test was run directly against a test CSV and existing processed Parquet data. A fully automated S3-to-Glue orchestration workflow has not been verified.



\## 3. AWS Components



| Service | Responsibility |

|---|---|

| Amazon S3 | Stores incoming, raw, processed, and quarantined transaction data |

| AWS IAM | Controls access to AWS resources |

| Amazon SQS | Buffers S3 object event notifications |

| AWS Lambda | Validates incoming CSV files and organizes records into date-based partitions |

| AWS Glue | Performs PySpark transformations, validation, and incremental processing |

| Glue Data Catalog | Maintains dataset schemas and table metadata |

| Amazon Athena | Executes SQL queries against data stored in S3 |

| Amazon CloudWatch | Provides execution logs for troubleshooting and validation |



\## 4. Data Processing



The Glue pipeline performs:



1\. Schema standardization and type conversion.

2\. Required-field and business-rule validation.

3\. Within-batch duplicate and conflict detection.

4\. Historical transaction comparison.

5\. Incremental processing of new transaction IDs.

6\. Quarantine of invalid or conflicting records.

7\. Record-count reconciliation before writing results.



\## 5. Data Quality and Historical Conflict Detection



A transaction ID is compared against previously processed transactions.



\- \*\*New transaction:\*\* Eligible for processed storage.

\- \*\*Identical historical duplicate:\*\* Skipped.

\- \*\*Historical conflict:\*\* Written to quarantine with a rejection reason.

\- \*\*Invalid record:\*\* Written to quarantine.



The pipeline preserves existing processed records rather than overwriting them when conflicting transaction values arrive.



\## 6. Lab 30 Verified Results



| Metric | Result |

|---|---|

| Total processed transactions | 3 |

| Unique processed transactions | 3 |

| Total processed amount | $700.75 |

| Historical transaction TXN30002 | $500.00 |

| Conflicting incoming TXN30002 | $750.00 |

| Quarantined conflict records | 1 |

| Reconciliation checks | All passed |



The quarantine record was verified in Athena with rejection reason `HISTORICAL\_TRANSACTION\_CONFLICT`.



\## 7. Implementation Limitations



This is a learning and portfolio project, not a production deployment.



Current limitations include:



\- The reconstructed main Glue script expects an existing processed Parquet dataset.

\- Append-mode writes do not provide complete idempotency or concurrent-write protection.

\- A fully automated end-to-end orchestration workflow has not been verified.

\- Additional automated testing, monitoring, security controls, and deployment automation would be required for production use.



\## 8. Future Enhancements



\- Add automated first-load initialization.

\- Introduce stronger idempotency and transaction handling.

\- Implement automated orchestration and alerting.

\- Add unit tests and integration tests.

\- Add infrastructure as code and CI/CD deployment.

\- Expand data quality monitoring and audit reporting.



