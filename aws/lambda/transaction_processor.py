import json
import urllib.parse
import boto3
import csv
import io
import os
import hashlib
from collections import defaultdict
from datetime import datetime

s3 = boto3.client("s3") 


def lambda_handler(event, context):
    print("Tandra Bank transaction processor started")

    for record in event["Records"]:

        print("SQS Message ID:")
        print(record["messageId"])

        # SQS body contains the S3 event
        s3_event = json.loads(record["body"])

        # Ignore special S3 test events
        if s3_event.get("Event") == "s3:TestEvent":
            print("Skipping S3 TestEvent")
            continue

        for s3_record in s3_event.get("Records", []):

            bucket_name = s3_record["s3"]["bucket"]["name"]

            object_key = s3_record["s3"]["object"]["key"]
            object_key = urllib.parse.unquote_plus(object_key)

            version_id = s3_record["s3"]["object"].get("versionId")

            print("S3 Bucket:")
            print(bucket_name)

            print("S3 Object Key:")
            print(object_key)

            print("S3 Version ID:")
            print(version_id)

            # Only process files from the incoming landing area
            if not object_key.startswith("incoming/transactions/"):
                print("Skipping non-incoming transaction object")
                continue

            # Read the exact S3 object version that generated the event
            get_object_args = {
                "Bucket": bucket_name,
                "Key": object_key
            }

            if version_id:
                get_object_args["VersionId"] = version_id

            response = s3.get_object(**get_object_args)

            file_content = response["Body"].read().decode("utf-8")

            # Parse incoming CSV
            csv_reader = csv.DictReader(io.StringIO(file_content))
            transactions = list(csv_reader)

            print("Number of incoming transactions:")
            print(len(transactions))

            transactions_by_date = defaultdict(list)
            invalid_transactions = []

            # Validate ALL transactions before writing anything
            for transaction in transactions:

                transaction_date = transaction.get(
                    "transaction_date", ""
                ).strip()

                amount = transaction.get(
                    "amount", ""
                ).strip()

                # Required-field validation
                if not transaction_date or not amount:
                    invalid_transactions.append(transaction)
                    continue

                # Validate that transaction_date is a real calendar date
                try:
                    parsed_date = datetime.strptime(
                        transaction_date,
                        "%Y-%m-%d"
                    )
                except ValueError:
                    invalid_transactions.append(transaction)
                    continue

                # Group transaction by validated business date
                transactions_by_date[
                    parsed_date.date()
                ].append(transaction)

            print("Invalid transactions:")
            for transaction in invalid_transactions:
                print(transaction)

            print("Validation summary:")
            print(f"Total: {len(transactions)}")
            print(
                f"Valid: "
                f"{len(transactions) - len(invalid_transactions)}"
            )
            print(f"Invalid: {len(invalid_transactions)}")

            # Fail the entire batch if any transaction is invalid
            if invalid_transactions:
                raise ValueError(
                    f"Validation failed: "
                    f"{len(invalid_transactions)} "
                    f"invalid transaction(s) found"
                )

            # Get original incoming filename
            incoming_filename = os.path.basename(object_key)
            incoming_stem = os.path.splitext(incoming_filename)[0]

            # ------------------------------------------------------
            # Create deterministic ID for this exact source version
            # ------------------------------------------------------

            source_identity = (
                f"{bucket_name}/"
                f"{object_key}/"
                f"{version_id if version_id else 'no-version'}"
            )

            source_id = hashlib.sha256(
                source_identity.encode("utf-8")
            ).hexdigest()[:12]

            print("Source ID:")
            print(source_id)

            # ------------------------------------------------------
            # Create one output CSV for each business date
            # ------------------------------------------------------

            for business_date, date_transactions in (
                transactions_by_date.items()
            ):

                year = business_date.strftime("%Y")
                month = business_date.strftime("%m")
                day = business_date.strftime("%d")

                # Hive-style S3 partition
                partition_prefix = (
                    f"raw/transactions/"
                    f"year={year}/"
                    f"month={month}/"
                    f"day={day}/"
                )

                # Unique deterministic output filename
                output_filename = (
                    f"{incoming_stem}_"
                    f"{source_id}.csv"
                )

                output_key = (
                    partition_prefix + output_filename
                )

                # Build output CSV in memory
                output_buffer = io.StringIO()

                csv_writer = csv.DictWriter(
                    output_buffer,
                    fieldnames=csv_reader.fieldnames
                )

                csv_writer.writeheader()
                csv_writer.writerows(date_transactions)

                # Write partitioned CSV to S3
                s3.put_object(
                    Bucket=bucket_name,
                    Key=output_key,
                    Body=output_buffer.getvalue().encode("utf-8"),
                    ContentType="text/csv"
                )

                print("Partition created:")
                print(output_key)

                print("Records written:")
                print(len(date_transactions))

    return {
        "statusCode": 200,
        "body": "Transactions partitioned successfully"
    }