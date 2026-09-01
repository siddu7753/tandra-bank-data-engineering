import os
import csv
import sys
from datetime import date

import boto3
import psycopg
from dotenv import load_dotenv
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    LoginRefreshRequired,
)


# --------------------------------------------------
# 1. LOAD ENVIRONMENT VARIABLES
# --------------------------------------------------

load_dotenv()

load_date = date.today().isoformat()

watermark_file = "control/transactions_watermark.txt"


# --------------------------------------------------
# 2. READ LAST SUCCESSFUL WATERMARK
# --------------------------------------------------

try:
    with open(watermark_file, "r") as file:
        last_transaction_id = int(
            file.read().strip()
        )

except (FileNotFoundError, ValueError) as error:
    print()
    print("EXTRACTION FAILED - WATERMARK")
    print(
        f"Unable to read a valid watermark "
        f"from {watermark_file}"
    )
    print(f"Reason: {error}")
    sys.exit(1)


print(
    f"Last processed transaction_id: "
    f"{last_transaction_id}"
)


# --------------------------------------------------
# 3. CONNECT TO POSTGRESQL
# --------------------------------------------------

try:
    connection = psycopg.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

    cursor = connection.cursor()

except psycopg.Error as error:
    print()
    print("EXTRACTION FAILED - POSTGRESQL")
    print("Unable to connect to PostgreSQL.")
    print(f"Reason: {error}")
    sys.exit(1)


# --------------------------------------------------
# 4. EXTRACT ONLY NEW TRANSACTIONS
# --------------------------------------------------

try:
    cursor.execute(
        """
        SELECT *
        FROM transactions
        WHERE transaction_id > %s
        ORDER BY transaction_id;
        """,
        (last_transaction_id,)
    )

    rows = cursor.fetchall()

except psycopg.Error as error:
    print()
    print("EXTRACTION FAILED - DATABASE QUERY")
    print(f"Reason: {error}")

    cursor.close()
    connection.close()

    sys.exit(1)


print(
    f"Extracted {len(rows)} transaction rows "
    f"from PostgreSQL."
)


# --------------------------------------------------
# 5. STOP CLEANLY IF THERE IS NO NEW DATA
# --------------------------------------------------

if len(rows) == 0:

    print(
        "No new transactions found. "
        "Pipeline finished with no data to load."
    )

    cursor.close()
    connection.close()

    sys.exit(0)


# --------------------------------------------------
# 6. GET COLUMN NAMES
# --------------------------------------------------

column_names = [
    desc.name
    for desc in cursor.description
]


# --------------------------------------------------
# 7. DATA QUALITY - NULL TRANSACTION ID
# --------------------------------------------------

if any(
    row[0] is None
    for row in rows
):
    print()
    print(
        "DATA QUALITY FAILED: "
        "NULL transaction_id found."
    )

    cursor.close()
    connection.close()

    sys.exit(1)


print(
    "Data quality passed: "
    "No NULL transaction IDs."
)


# --------------------------------------------------
# 8. DATA QUALITY - DUPLICATE TRANSACTION ID
# --------------------------------------------------

transaction_ids = [
    row[0]
    for row in rows
]

if (
    len(transaction_ids)
    != len(set(transaction_ids))
):
    print()
    print(
        "DATA QUALITY FAILED: "
        "Duplicate transaction_id found."
    )

    cursor.close()
    connection.close()

    sys.exit(1)


print(
    "Data quality passed: "
    "No duplicate transaction IDs."
)


# --------------------------------------------------
# 9. DATA QUALITY - NULL AMOUNT
# --------------------------------------------------

amount_index = column_names.index(
    "amount"
)

if any(
    row[amount_index] is None
    for row in rows
):
    print()
    print(
        "DATA QUALITY FAILED: "
        "NULL amount found."
    )

    cursor.close()
    connection.close()

    sys.exit(1)


print(
    "Data quality passed: "
    "No NULL amounts."
)


# --------------------------------------------------
# 10. CREATE UNIQUE RAW CSV FILE
# --------------------------------------------------

first_transaction_id = rows[0][0]
last_transaction_id_new = rows[-1][0]

file_name = (
    f"transactions_"
    f"{first_transaction_id}_"
    f"{last_transaction_id_new}.csv"
)

output_file = os.path.join(
    "data",
    file_name
)


# --------------------------------------------------
# 11. WRITE RAW CSV
# --------------------------------------------------

try:
    with open(
        output_file,
        "w",
        newline=""
    ) as file:

        writer = csv.writer(file)

        writer.writerow(
            column_names
        )

        writer.writerows(
            rows
        )

except OSError as error:
    print()
    print("EXTRACTION FAILED - LOCAL FILE")
    print(
        f"Unable to create raw CSV: "
        f"{output_file}"
    )
    print(f"Reason: {error}")

    cursor.close()
    connection.close()

    sys.exit(1)


print(
    f"Raw CSV created: "
    f"{output_file}"
)


# --------------------------------------------------
# 12. PREPARE RAW S3 DESTINATION
# --------------------------------------------------

bucket_name = "tandra-bank-data"

s3_key = (
    "raw/transactions/"
    f"load_date={load_date}/"
    f"{file_name}"
)


# --------------------------------------------------
# 13. UPLOAD RAW CSV TO S3
# --------------------------------------------------

try:

    s3 = boto3.client(
        "s3",
        region_name="us-east-2"
    )

    s3.upload_file(
        output_file,
        bucket_name,
        s3_key
    )


except (
    LoginRefreshRequired,
    NoCredentialsError
) as error:

    print()
    print("=" * 60)
    print("EXTRACTION FAILED - AWS AUTHENTICATION")
    print("=" * 60)

    print(
        "AWS credentials are missing or "
        "the login session has expired."
    )

    print()
    print("Run:")
    print("aws login")

    print()
    print(
        f"Technical reason: {error}"
    )

    print()
    print(
        "No transaction watermark "
        "was advanced."
    )

    cursor.close()
    connection.close()

    sys.exit(1)


except ClientError as error:

    error_code = (
        error.response
        .get("Error", {})
        .get("Code", "Unknown")
    )

    error_message = (
        error.response
        .get("Error", {})
        .get("Message", str(error))
    )

    print()
    print("=" * 60)
    print("EXTRACTION FAILED - AWS S3")
    print("=" * 60)

    print(
        f"AWS error code: "
        f"{error_code}"
    )

    print(
        f"AWS error message: "
        f"{error_message}"
    )

    print()
    print(
        "No transaction watermark "
        "was advanced."
    )

    cursor.close()
    connection.close()

    sys.exit(1)


except BotoCoreError as error:

    print()
    print("=" * 60)
    print("EXTRACTION FAILED - AWS")
    print("=" * 60)

    print(
        f"AWS SDK error: {error}"
    )

    print()
    print(
        "No transaction watermark "
        "was advanced."
    )

    cursor.close()
    connection.close()

    sys.exit(1)


except Exception as error:

    print()
    print("=" * 60)
    print("EXTRACTION FAILED - UNEXPECTED ERROR")
    print("=" * 60)

    print(
        f"Unexpected S3 upload error: "
        f"{error}"
    )

    print()
    print(
        "No transaction watermark "
        "was advanced."
    )

    cursor.close()
    connection.close()

    sys.exit(1)


print(
    f"Uploaded successfully to "
    f"s3://{bucket_name}/{s3_key}"
)


# --------------------------------------------------
# 14. CLOSE DATABASE CONNECTION
# --------------------------------------------------

cursor.close()
connection.close()


# --------------------------------------------------
# IMPORTANT
# --------------------------------------------------
#
# This script intentionally DOES NOT update the
# transaction watermark.
#
# run_pipeline.py updates the watermark only after:
#
# Extract
#   -> Transform
#   -> Parquet
#   -> Processed S3 upload
#
# all succeed.
#
# This prevents transaction loss when a downstream
# pipeline step fails.
# --------------------------------------------------

print(
    "Extraction completed successfully."
)