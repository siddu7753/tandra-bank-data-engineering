import os
import random
import psycopg

from dotenv import load_dotenv


# -----------------------------------------
# 1. Load environment variables
# -----------------------------------------

load_dotenv()


# -----------------------------------------
# 2. Synthetic data configuration
# -----------------------------------------

account_ids = [5001, 5002, 5003]

merchants = [
    "Walmart",
    "Target",
    "Costco",
    "Starbucks",
    "Chipotle",
    "Amazon",
    "Shell",
    "Walgreens"
]

transaction_types = [
    "CARD",
    "ACH"
]

statuses = [
    "COMPLETED",
    "PENDING"
]

number_of_transactions = 1000

start_transaction_id = 90021


# -----------------------------------------
# 3. Connect to PostgreSQL
# -----------------------------------------

connection = psycopg.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

cursor = connection.cursor()


# -----------------------------------------
# 4. Generate and insert transactions
# -----------------------------------------

print(
    f"Preparing to generate "
    f"{number_of_transactions} synthetic transactions."
)

for i in range(number_of_transactions):

    transaction_id = start_transaction_id + i

    account_id = random.choice(account_ids)

    merchant_name = random.choice(merchants)

    amount = round(
        random.uniform(1.00, 500.00),
        2
    )

    transaction_type = random.choice(
        transaction_types
    )

    status = random.choice(
        statuses
    )

    cursor.execute("""
        INSERT INTO transactions (
            transaction_id,
            account_id,
            merchant_name,
            amount,
            transaction_type,
            status,
            transaction_time
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            CURRENT_TIMESTAMP
        );
    """, (
        transaction_id,
        account_id,
        merchant_name,
        amount,
        transaction_type,
        status
    ))


# -----------------------------------------
# 5. Commit all 1,000 rows
# -----------------------------------------

connection.commit()


# -----------------------------------------
# 6. Close database resources
# -----------------------------------------

cursor.close()
connection.close()


# -----------------------------------------
# 7. Success message
# -----------------------------------------

last_transaction_id = (
    start_transaction_id
    + number_of_transactions
    - 1
)

print(
    "Synthetic transactions inserted successfully."
)

print(
    f"Transaction ID range: "
    f"{start_transaction_id} to {last_transaction_id}"
)