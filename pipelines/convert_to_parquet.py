import glob
import os
import sys

import pyarrow as pa
import pyarrow.compute
import pyarrow.csv as csv
import pyarrow.parquet as parquet


# --------------------------------------------------
# 1. FIND CLEAN TRANSACTION CSV FILES
# --------------------------------------------------
# If a specific clean CSV is passed in, use it.
# Otherwise, automatically use the newest clean CSV.
if len(sys.argv) > 1:
    input_file = sys.argv[1]

    if not os.path.exists(input_file):
        raise FileNotFoundError(
            f"Input file not found: {input_file}"
        )
else:
    files = glob.glob(
        "data/clean_transactions_*.csv"
    )

    if not files:
        raise FileNotFoundError(
            "No clean transaction CSV files found "
            "in data folder."
        )

    input_file = max(
        files,
        key=os.path.getmtime
    )
# Example:
# clean_transactions_91027_91036.csv
# becomes:
# 91027_91036

filename = os.path.basename(input_file)

batch_name = (
    filename
    .replace("clean_transactions_", "")
    .replace(".csv", "")
)


# --------------------------------------------------
# 3. CREATE MATCHING PARQUET FILE NAME
# --------------------------------------------------

output_file = (
    f"data/clean_transactions_"
    f"{batch_name}.parquet"
)

print(
    f"Latest clean file detected: "
    f"{input_file}"
)

print(
    f"Parquet output will be: "
    f"{output_file}"
)


# --------------------------------------------------
# 4. READ CLEANED CSV
# --------------------------------------------------

table = csv.read_csv(
    input_file
)


# --------------------------------------------------
# 5. CONVERT TRANSACTION_TIME TO MILLISECONDS
# --------------------------------------------------
#
# PyArrow originally inferred transaction_time as:
#
# timestamp[ns]
#
# AWS Glue cataloged that Parquet timestamp as BIGINT.
#
# We explicitly convert it to millisecond precision
# so Glue/Athena can work with it as a timestamp.
# --------------------------------------------------

transaction_time_index = (
    table.schema.get_field_index(
        "transaction_time"
    )
)

if transaction_time_index == -1:
    raise ValueError(
        "transaction_time column was not found."
    )

transaction_time_ms = pa.compute.cast(
    table.column("transaction_time"),
    pa.timestamp("ms"),
    safe=False
)
table = table.set_column(
    transaction_time_index,
    "transaction_time",
    transaction_time_ms
)


# --------------------------------------------------
# 6. WRITE PARQUET FILE
# --------------------------------------------------

parquet.write_table(
    table,
    output_file
)


# --------------------------------------------------
# 7. DISPLAY RESULTS
# --------------------------------------------------

print(
    f"Parquet file created: "
    f"{output_file}"
)

print(
    f"Rows: "
    f"{table.num_rows}"
)

print(
    f"Columns: "
    f"{table.num_columns}"
)

print()

print(
    "Parquet schema:"
)

print(
    table.schema
)