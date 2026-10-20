\# Tandra Bank — Lab 30 Data Quality and Validation Report



\## 1. Objective



Validate the AWS Glue transaction processing pipeline, including incremental loading, duplicate detection, historical conflict handling, quarantine processing, and reconciliation.



\*\*Test date:\*\* October 18, 2026



\*\*Technology:\*\* AWS Glue, PySpark, Amazon S3, Amazon Athena, and Parquet.



\## 2. Test Scenarios



| Scenario | Expected Behavior | Result |

|---|---|---|

| Initial transaction processing | Write three valid transactions to processed Parquet | Passed |

| Historical duplicate detection | Skip previously processed identical transactions | Passed |

| Historical conflict detection | Identify an existing transaction ID with changed business values | Passed |

| Quarantine processing | Write conflicting transaction to quarantine Parquet | Passed |

| Historical data integrity | Preserve the original processed transaction | Passed |

| Reconciliation | Confirm record counts balance | Passed |



\## 3. Processed Data Validation



Amazon Athena returned:



| Metric | Actual Result |

|---|---:|

| Total processed records | 3 |

| Unique transactions | 3 |

| Total transaction amount | $700.75 |



The processed dataset contained three unique transactions.



\## 4. Historical Conflict Test



An incoming transaction was created with the same transaction ID as an existing processed record, but with a different amount.



| Field | Historical Record | Incoming Record |

|---|---|---|

| Transaction ID | TXN30002 | TXN30002 |

| Account ID | 1002 | 1002 |

| Amount | $500.00 | $750.00 |

| Merchant | Payroll | Payroll |



\*\*Expected behavior:\*\* Preserve the historical record and quarantine the conflicting incoming record.



\## 5. AWS Glue Execution Results



```text

Raw transactions: 1

Valid transactions: 1

Invalid transactions: 0

Batch conflicts: 0

Batch duplicates skipped: 0

New transactions: 0

Identical historical duplicates: 0

Historical conflicts: 1

Total quarantine records: 1



Initial reconciliation: True

Batch reconciliation: True

Historical reconciliation: True

Historical classification: True

Quarantine reconciliation: True



No new transactions to write.

Quarantine records written: 1

```



\## 6. Athena Quarantine Verification



Athena confirmed:



| transaction\_id | account\_id | amount | merchant | rejection\_reason |

|---|---|---:|---|---|

| TXN30002 | 1002 | 750.00 | Payroll | HISTORICAL\_TRANSACTION\_CONFLICT |



\## 7. Historical Data Integrity Verification



Athena confirmed the original processed transaction remained unchanged:



| transaction\_id | account\_id | amount | merchant |

|---|---|---:|---|

| TXN30002 | 1002 | 500.00 | Payroll |



This verifies that the conflicting incoming transaction did not overwrite the existing processed record.



\## 8. Conclusion



All recorded Lab 30 functional validation scenarios passed.



The pipeline successfully demonstrated:



\- Transaction data validation

\- Historical duplicate detection

\- Historical conflict identification

\- Quarantine output generation

\- Preservation of existing processed records

\- Record-count reconciliation

\- SQL-based verification through Amazon Athena



These results validate the tested lab scenarios. They do not establish production readiness, complete idempotency, or performance at scale.



\## 9. Future Testing



Additional recommended tests include:



\- Repeated quarantine processing to detect duplicate writes

\- Concurrent job execution

\- Empty historical dataset initialization

\- Large transaction batches

\- Schema changes and malformed source files

\- Automated regression tests



