import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    expr,
    trim,
    date_format,
    when,
    lit,
    countDistinct,
    struct
)

# ==================================================
# 1. INITIALIZE AWS GLUE
# ==================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ==================================================
# 2. CONFIGURE S3 PATHS
# ==================================================

bucket = "tandra-bank-data-siddu-2026"

test_input_path = (
    f"s3://{bucket}/lab30/test-input/"
    "historical-conflict/historical_conflict_test.csv"
)

processed_path = (
    f"s3://{bucket}/lab30/processed/transactions/"
)

quarantine_path = (
    f"s3://{bucket}/lab30/quarantine/transactions/"
)


# ==================================================
# 3. READ TEST CSV
# ==================================================

raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(test_input_path)
)


# ==================================================
# 4. STANDARDIZE DATA
# ==================================================

standardized_df = (
    raw_df
    .withColumn(
        "transaction_id",
        trim(col("transaction_id"))
    )
    .withColumn(
        "account_id",
        expr("try_cast(account_id AS BIGINT)")
    )
    .withColumn(
        "transaction_date",
        expr(
            "CAST(try_to_timestamp(transaction_date, "
            "'yyyy-MM-dd') AS DATE)"
        )
    )
    .withColumn(
        "transaction_type",
        trim(col("transaction_type"))
    )
    .withColumn(
        "amount",
        expr("try_cast(amount AS DECIMAL(12,2))")
    )
    .withColumn(
        "merchant",
        trim(col("merchant"))
    )
    .withColumn(
        "year",
        date_format(col("transaction_date"), "yyyy")
    )
    .withColumn(
        "month",
        date_format(col("transaction_date"), "MM")
    )
    .withColumn(
        "day",
        date_format(col("transaction_date"), "dd")
    )
)


# ==================================================
# 5. DATA QUALITY VALIDATION
# ==================================================

validated_df = standardized_df.withColumn(
    "rejection_reason",
    when(
        col("transaction_id").isNull()
        | (col("transaction_id") == ""),
        lit("INVALID_TRANSACTION_ID")
    )
    .when(
        col("account_id").isNull(),
        lit("INVALID_ACCOUNT_ID")
    )
    .when(
        col("transaction_date").isNull(),
        lit("INVALID_TRANSACTION_DATE")
    )
    .when(
        col("transaction_type").isNull()
        | (~col("transaction_type").isin("DEBIT", "CREDIT")),
        lit("INVALID_TRANSACTION_TYPE")
    )
    .when(
        col("amount").isNull()
        | (col("amount") <= 0),
        lit("INVALID_AMOUNT")
    )
    .when(
        col("merchant").isNull()
        | (col("merchant") == ""),
        lit("INVALID_MERCHANT")
    )
    .otherwise(lit(None))
)

valid_df = validated_df.filter(
    col("rejection_reason").isNull()
).drop("rejection_reason")

invalid_df = validated_df.filter(
    col("rejection_reason").isNotNull()
)


# ==================================================
# 6. WITHIN-BATCH DUPLICATE DETECTION
# ==================================================

business_columns = [
    "account_id",
    "transaction_date",
    "transaction_type",
    "amount",
    "merchant"
]

conflicting_ids = (
    valid_df
    .groupBy("transaction_id")
    .agg(
        countDistinct(
            struct(*[col(c) for c in business_columns])
        ).alias("distinct_versions")
    )
    .filter(col("distinct_versions") > 1)
    .select("transaction_id")
)

batch_conflicts_df = (
    valid_df
    .join(
        conflicting_ids,
        "transaction_id",
        "inner"
    )
    .withColumn(
        "rejection_reason",
        lit("BATCH_TRANSACTION_CONFLICT")
    )
)

non_conflicting_df = valid_df.join(
    conflicting_ids,
    "transaction_id",
    "left_anti"
)

unique_df = non_conflicting_df.dropDuplicates(
    ["transaction_id"]
)


# ==================================================
# 7. READ HISTORICAL PARQUET
# ==================================================

historical_df = spark.read.parquet(processed_path)

historical_ids = historical_df.select(
    "transaction_id"
).distinct()


# ==================================================
# 8. CLASSIFY HISTORICAL TRANSACTIONS
# ==================================================

new_df = unique_df.join(
    historical_ids,
    "transaction_id",
    "left_anti"
)

existing_df = unique_df.join(
    historical_ids,
    "transaction_id",
    "inner"
)

incoming = existing_df.alias("incoming")
historical = historical_df.alias("historical")

comparison = incoming.join(
    historical,
    col("incoming.transaction_id")
    == col("historical.transaction_id"),
    "inner"
)

same_values = (
    col("incoming.account_id").eqNullSafe(
        col("historical.account_id")
    )
    & col("incoming.transaction_date").eqNullSafe(
        col("historical.transaction_date")
    )
    & col("incoming.transaction_type").eqNullSafe(
        col("historical.transaction_type")
    )
    & col("incoming.amount").eqNullSafe(
        col("historical.amount")
    )
    & col("incoming.merchant").eqNullSafe(
        col("historical.merchant")
    )
)

identical_df = (
    comparison
    .filter(same_values)
    .select("incoming.*")
)

historical_conflicts_df = (
    comparison
    .filter(~same_values)
    .select("incoming.*")
    .withColumn(
        "rejection_reason",
        lit("HISTORICAL_TRANSACTION_CONFLICT")
    )
)


# ==================================================
# 9. COMBINE QUARANTINE RECORDS
# ==================================================

quarantine_df = (
    invalid_df
    .unionByName(
        batch_conflicts_df,
        allowMissingColumns=True
    )
    .unionByName(
        historical_conflicts_df,
        allowMissingColumns=True
    )
)


# ==================================================
# 10. RECONCILIATION COUNTS
# ==================================================

raw_count = raw_df.count()
valid_count = valid_df.count()
invalid_count = invalid_df.count()

batch_conflict_count = batch_conflicts_df.count()
non_conflicting_count = non_conflicting_df.count()
unique_count = unique_df.count()

batch_duplicate_count = (
    non_conflicting_count - unique_count
)

new_count = new_df.count()
existing_count = existing_df.count()
identical_count = identical_df.count()
historical_conflict_count = historical_conflicts_df.count()

quarantine_count = quarantine_df.count()

initial_reconciliation = (
    raw_count == valid_count + invalid_count
)

batch_reconciliation = (
    raw_count == (
        invalid_count
        + batch_conflict_count
        + batch_duplicate_count
        + unique_count
    )
)

historical_reconciliation = (
    unique_count == new_count + existing_count
)

historical_classification = (
    existing_count
    == identical_count + historical_conflict_count
)

quarantine_reconciliation = (
    quarantine_count
    == invalid_count
    + batch_conflict_count
    + historical_conflict_count
)

print("========================================")
print("LAB 30 — HISTORICAL CONFLICT TEST")
print("========================================")

print("Raw transactions:", raw_count)
print("Valid transactions:", valid_count)
print("Invalid transactions:", invalid_count)
print("Batch conflicts:", batch_conflict_count)
print("Batch duplicates skipped:", batch_duplicate_count)

print("New transactions:", new_count)
print("Identical historical duplicates:", identical_count)
print("Historical conflicts:", historical_conflict_count)
print("Total quarantine records:", quarantine_count)

print("Initial reconciliation:", initial_reconciliation)
print("Batch reconciliation:", batch_reconciliation)
print("Historical reconciliation:", historical_reconciliation)
print("Historical classification:", historical_classification)
print("Quarantine reconciliation:", quarantine_reconciliation)


# ==================================================
# 11. STOP IF RECONCILIATION FAILS
# ==================================================

if not all([
    initial_reconciliation,
    batch_reconciliation,
    historical_reconciliation,
    historical_classification,
    quarantine_reconciliation
]):
    raise Exception(
        "LAB 30 RECONCILIATION FAILED."
    )


# ==================================================
# 12. WRITE NEW TRANSACTIONS
# ==================================================

if new_count > 0:

    (
        new_df.write
        .mode("append")
        .parquet(processed_path)
    )

    print("New processed transactions:", new_count)

else:

    print("No new transactions to write.")


# ==================================================
# 13. WRITE QUARANTINE RECORDS
# ==================================================

if quarantine_count > 0:

    (
        quarantine_df.write
        .mode("append")
        .parquet(quarantine_path)
    )

    print("Quarantine records written:", quarantine_count)

else:

    print("No quarantine records to write.")


# ==================================================
# 14. COMMIT JOB
# ==================================================

job.commit()

print("LAB 30 HISTORICAL CONFLICT TEST COMPLETED")
