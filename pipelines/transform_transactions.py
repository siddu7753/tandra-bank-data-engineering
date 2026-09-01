import csv
import glob
import os

# Find all raw transaction CSV files
files = glob.glob("data/transactions_*.csv")

if not files:
    raise FileNotFoundError("No transaction CSV files found in data folder.")

# Select the most recently created/modified transaction file
input_file = max(files, key=os.path.getmtime)

# Get the batch portion of the filename
# Example:
# transactions_91027_91036.csv
# becomes:
# 91027_91036
filename = os.path.basename(input_file)
batch_name = filename.replace("transactions_", "").replace(".csv", "")

# Automatically create the corresponding clean filename
output_file = f"data/clean_transactions_{batch_name}.csv"

print(f"Latest raw file detected: {input_file}")
print(f"Output file will be: {output_file}")

clean_rows = []

with open(input_file, "r") as file:
    reader = csv.DictReader(file)

    for row in reader:
        row["merchant_name"] = row["merchant_name"].strip().upper()
        row["transaction_date"] = row["transaction_time"][:10]
        clean_rows.append(row)

print(f"Rows read from raw file: {len(clean_rows)}")

if clean_rows:
    print(f"First merchant: {clean_rows[0]['merchant_name']}")
    print(f"First transaction date: {clean_rows[0]['transaction_date']}")

fieldnames = [
    "transaction_id",
    "account_id",
    "merchant_name",
    "amount",
    "transaction_type",
    "status",
    "transaction_time",
    "transaction_date"
]

with open(output_file, "w", newline="") as file:
    writer = csv.DictWriter(
        file,
        fieldnames=fieldnames
    )

    writer.writeheader()
    writer.writerows(clean_rows)

print(f"Processed file created: {output_file}")