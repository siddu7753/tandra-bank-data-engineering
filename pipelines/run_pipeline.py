import subprocess
import sys
import glob
import os
from datetime import date

import boto3
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    LoginRefreshRequired,
)


# --------------------------------------------------
# HELPER: RUN A PIPELINE SCRIPT
# --------------------------------------------------

def run_step(script_name):
    print()
    print("=" * 60)
    print(f"Running: {script_name}")
    print("=" * 60)

    result = subprocess.run(
        [sys.executable, script_name],
        check=False
    )

    if result.returncode != 0:
        print()
        print("=" * 60)
        print("PIPELINE FAILED")
        print("=" * 60)
        print(f"Failed step: {script_name}")
        print(
            "Watermark will not be advanced by "
            "the orchestrator."
        )
        sys.exit(result.returncode)


# --------------------------------------------------
# START PIPELINE
# --------------------------------------------------

print()
print("=" * 60)
print("TANDRA BANK TRANSACTION PIPELINE")
print("=" * 60)


# --------------------------------------------------
# 1. READ CURRENT WATERMARK
# --------------------------------------------------

watermark_file = "control/transactions_watermark.txt"

try:
    with open(watermark_file, "r") as file:
        watermark_before = int(
            file.read().strip()
        )

except (FileNotFoundError, ValueError) as error:
    print()
    print("PIPELINE FAILED")
    print(
        f"Unable to read a valid watermark from "
        f"{watermark_file}"
    )
    print(f"Reason: {error}")
    sys.exit(1)

print(
    f"Current watermark: "
    f"{watermark_before}"
)


# --------------------------------------------------
# 2. RECORD RAW FILE STATE BEFORE EXTRACTION
# --------------------------------------------------

raw_files_before = glob.glob(
    "data/transactions_*.csv"
)

latest_raw_before = None
latest_raw_before_mtime = None

if raw_files_before:

    latest_raw_before = max(
        raw_files_before,
        key=os.path.getmtime
    )

    latest_raw_before_mtime = os.path.getmtime(
        latest_raw_before
    )


# --------------------------------------------------
# 3. EXTRACT NEW TRANSACTIONS
# --------------------------------------------------

run_step(
    "pipelines/extract_transactions.py"
)


# --------------------------------------------------
# 4. DETECT WHETHER A NEW RAW BATCH WAS CREATED
# --------------------------------------------------

raw_files_after = glob.glob(
    "data/transactions_*.csv"
)

if not raw_files_after:

    print()
    print(
        "No raw transaction files found."
    )

    print(
        "Pipeline finished with nothing "
        "new to process."
    )

    sys.exit(0)


latest_raw_after = max(
    raw_files_after,
    key=os.path.getmtime
)

latest_raw_after_mtime = os.path.getmtime(
    latest_raw_after
)


if (
    latest_raw_before == latest_raw_after
    and
    latest_raw_before_mtime
    == latest_raw_after_mtime
):

    print()
    print(
        "No new transactions were extracted."
    )

    print(
        "Pipeline finished with nothing "
        "new to process."
    )

    sys.exit(0)


print()
print(
    f"New raw batch detected: "
    f"{latest_raw_after}"
)


# --------------------------------------------------
# 5. TRANSFORM NEW RAW BATCH
# --------------------------------------------------

run_step(
    "pipelines/transform_transactions.py"
)


# --------------------------------------------------
# 6. CONVERT CLEAN CSV TO PARQUET
# --------------------------------------------------

run_step(
    "pipelines/convert_to_parquet.py"
)


# --------------------------------------------------
# 7. FIND NEWEST PARQUET FILE
# --------------------------------------------------

parquet_files = glob.glob(
    "data/clean_transactions_*.parquet"
)

if not parquet_files:

    print()
    print("PIPELINE FAILED")
    print(
        "No processed Parquet file was found."
    )

    print(
        "Watermark was not updated."
    )

    sys.exit(1)


parquet_file = max(
    parquet_files,
    key=os.path.getmtime
)

file_name = os.path.basename(
    parquet_file
)

print()
print(
    f"Latest Parquet file detected: "
    f"{parquet_file}"
)


# --------------------------------------------------
# 8. PREPARE PROCESSED S3 DESTINATION
# --------------------------------------------------

load_date = date.today().isoformat()

bucket_name = "tandra-bank-data"

s3_key = (
    "processed/transactions_parquet/"
    f"load_date={load_date}/"
    f"{file_name}"
)

print()
print(
    "Processed S3 destination:"
)

print(
    f"s3://{bucket_name}/{s3_key}"
)


# --------------------------------------------------
# 9. UPLOAD PROCESSED PARQUET TO S3
# --------------------------------------------------

try:

    s3 = boto3.client(
        "s3",
        region_name="us-east-2"
    )

    print()
    print(
        "Uploading processed Parquet "
        "file to S3..."
    )

    s3.upload_file(
        parquet_file,
        bucket_name,
        s3_key
    )


except (
    LoginRefreshRequired,
    NoCredentialsError
) as error:

    print()
    print("=" * 60)
    print("PIPELINE FAILED - AWS AUTHENTICATION")
    print("=" * 60)

    print(
        "AWS credentials are missing or "
        "the login session has expired."
    )

    print()
    print(
        "Run this command and then retry:"
    )

    print(
        "aws login"
    )

    print()
    print(
        f"Technical reason: {error}"
    )

    print()
    print(
        f"Watermark remains: "
        f"{watermark_before}"
    )

    sys.exit(1)


except ClientError as error:

    print()
    print("=" * 60)
    print("PIPELINE FAILED - AWS S3")
    print("=" * 60)

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
        f"Watermark remains: "
        f"{watermark_before}"
    )

    sys.exit(1)


except BotoCoreError as error:

    print()
    print("=" * 60)
    print("PIPELINE FAILED - AWS")
    print("=" * 60)

    print(
        f"AWS SDK error: "
        f"{error}"
    )

    print()
    print(
        f"Watermark remains: "
        f"{watermark_before}"
    )

    sys.exit(1)


except Exception as error:

    print()
    print("=" * 60)
    print("PIPELINE FAILED - UNEXPECTED ERROR")
    print("=" * 60)

    print(
        f"Unexpected upload error: "
        f"{error}"
    )

    print()
    print(
        f"Watermark remains: "
        f"{watermark_before}"
    )

    sys.exit(1)


print()
print(
    "Processed Parquet uploaded "
    "successfully."
)


# --------------------------------------------------
# 10. DETERMINE SUCCESSFUL BATCH WATERMARK
# --------------------------------------------------

batch_name = (
    file_name
    .replace(
        "clean_transactions_",
        ""
    )
    .replace(
        ".parquet",
        ""
    )
)

try:

    first_id, last_id = (
        batch_name.split("_")
    )

    int(first_id)
    int(last_id)

except (ValueError, TypeError):

    print()
    print("=" * 60)
    print("PIPELINE FAILED")
    print("=" * 60)

    print(
        "Unable to determine transaction "
        "IDs from the Parquet filename."
    )

    print(
        f"Filename: {file_name}"
    )

    print()
    print(
        f"Watermark remains: "
        f"{watermark_before}"
    )

    sys.exit(1)


# --------------------------------------------------
# 11. UPDATE WATERMARK ONLY AFTER S3 SUCCESS
# --------------------------------------------------

try:

    with open(
        watermark_file,
        "w"
    ) as file:

        file.write(
            str(last_id)
        )

except OSError as error:

    print()
    print("=" * 60)
    print("PIPELINE FAILED - WATERMARK UPDATE")
    print("=" * 60)

    print(
        f"Processed data reached S3, but "
        f"the watermark file could not "
        f"be updated."
    )

    print(
        f"Reason: {error}"
    )

    print()
    print(
        "Do not create another transaction "
        "batch until this is corrected."
    )

    sys.exit(1)


print(
    f"Watermark updated: "
    f"{watermark_before} -> {last_id}"
)


# --------------------------------------------------
# 12. COMPLETE LOCAL PIPELINE
# --------------------------------------------------

print()
print("=" * 60)
print("LOCAL PIPELINE COMPLETED SUCCESSFULLY")
print("=" * 60)

print(
    f"Processed batch: "
    f"{first_id}-{last_id}"
)

print(
    f"Processed file: "
    f"{file_name}"
)

print(
    f"Current watermark: "
    f"{last_id}"
)

print()
print(
    "The processed S3 upload should "
    "trigger EventBridge and the "
    "AWS Glue workflow automatically."
)