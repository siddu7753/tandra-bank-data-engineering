import os
from decimal import Decimal, InvalidOperation

import psycopg
from flask import Flask, render_template, request, redirect, url_for
from dotenv import load_dotenv
from werkzeug.security import check_password_hash


# Load database credentials from the .env file
load_dotenv()

# Create the Flask application
app = Flask(__name__)


# --------------------------------------------------
# DATABASE CONNECTION
# --------------------------------------------------

def get_connection():
    return psycopg.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )


# --------------------------------------------------
# LOGIN PAGE
# --------------------------------------------------

@app.route("/")
def home():
    return render_template("login.html")


# --------------------------------------------------
# LOGIN
# --------------------------------------------------

@app.route("/login", methods=["POST"])
def login():

    username = request.form["username"]
    password = request.form["password"]

    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute("""
        SELECT
            customer_id,
            password_hash
        FROM customers
        WHERE username = %s;
    """, (username,))

    customer = cursor.fetchone()

    cursor.close()
    connection.close()

    if customer is None:
        return render_template(
            "login.html",
            error="Invalid username or password"
        )

    customer_id = customer[0]
    stored_hash = customer[1]

    if not check_password_hash(stored_hash, password):
        return render_template(
            "login.html",
            error="Invalid username or password"
        )

    return redirect(
        url_for(
            "customer_dashboard",
            customer_id=customer_id
        )
    )


# --------------------------------------------------
# CUSTOMER DASHBOARD
# --------------------------------------------------

@app.route("/customer/<int:customer_id>")
def customer_dashboard(customer_id):

    message = request.args.get("message")

    connection = get_connection()
    cursor = connection.cursor()

    # Get customer's accounts
    cursor.execute("""
        SELECT
            c.first_name,
            c.last_name,
            a.account_type,
            a.balance,
            a.account_id
        FROM customers c
        JOIN accounts a
            ON c.customer_id = a.customer_id
        WHERE c.customer_id = %s
        ORDER BY a.account_id;
    """, (customer_id,))

    accounts = cursor.fetchall()

    # Get customer's transactions
    cursor.execute("""
        SELECT
            t.merchant_name,
            t.amount,
            t.transaction_type,
            t.status,
            t.transaction_time,
            a.account_type
        FROM transactions t
        JOIN accounts a
            ON t.account_id = a.account_id
        WHERE a.customer_id = %s
        ORDER BY t.transaction_time DESC;
    """, (customer_id,))

    transactions = cursor.fetchall()

    # Get customer's expenses
    cursor.execute("""
        SELECT
            merchant,
            amount,
            category,
            expense_date
        FROM expenses
        WHERE customer_id = %s
        ORDER BY expense_date DESC, expense_id DESC;
    """, (customer_id,))

    expenses = cursor.fetchall()

    cursor.close()
    connection.close()

    if not accounts:
        return "Customer not found", 404

    customer_name = accounts[0][0] + " " + accounts[0][1]

    return render_template(
        "dashboard.html",
        customer_name=customer_name,
        customer_id=customer_id,
        accounts=accounts,
        transactions=transactions,
        expenses=expenses,
        message=message
    )


# --------------------------------------------------
# TRANSFER MONEY
# --------------------------------------------------

@app.route("/transfer", methods=["POST"])
def transfer():

    customer_id = int(request.form["customer_id"])
    from_account = int(request.form["from_account"])
    to_account = int(request.form["to_account"])

    try:
        amount = Decimal(request.form["amount"])
    except InvalidOperation:
        return "Invalid transfer amount", 400

    if amount <= 0:
        return "Transfer amount must be greater than zero", 400

    if from_account == to_account:
        return "From and To accounts cannot be the same", 400

    connection = get_connection()

    try:
        cursor = connection.cursor()

        # Get and lock both accounts
        cursor.execute("""
            SELECT
                account_id,
                balance
            FROM accounts
            WHERE customer_id = %s
              AND account_id IN (%s, %s)
            FOR UPDATE;
        """, (
            customer_id,
            from_account,
            to_account
        ))

        account_rows = cursor.fetchall()

        if len(account_rows) != 2:
            connection.rollback()
            return "Invalid account selection", 400

        balances = {
            row[0]: row[1]
            for row in account_rows
        }

        # Check available balance
        if balances[from_account] < amount:
            connection.rollback()
            return "Insufficient funds", 400

        # Subtract from source account
        cursor.execute("""
            UPDATE accounts
            SET balance = balance - %s
            WHERE account_id = %s;
        """, (
            amount,
            from_account
        ))

        # Add to destination account
        cursor.execute("""
            UPDATE accounts
            SET balance = balance + %s
            WHERE account_id = %s;
        """, (
            amount,
            to_account
        ))

        # Generate transaction IDs
        cursor.execute("""
            SELECT
                COALESCE(MAX(transaction_id), 90000) + 1
            FROM transactions;
        """)

        first_transaction_id = cursor.fetchone()[0]
        second_transaction_id = first_transaction_id + 1

        # Record outgoing transfer
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
                'TRANSFER_OUT',
                'COMPLETED',
                CURRENT_TIMESTAMP
            );
        """, (
            first_transaction_id,
            from_account,
            f"Transfer to account {to_account}",
            amount
        ))

        # Record incoming transfer
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
                'TRANSFER_IN',
                'COMPLETED',
                CURRENT_TIMESTAMP
            );
        """, (
            second_transaction_id,
            to_account,
            f"Transfer from account {from_account}",
            amount
        ))

        connection.commit()

        cursor.close()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

    return redirect(
        url_for(
            "customer_dashboard",
            customer_id=customer_id,
            message=f"${amount:.2f} transferred successfully!"
        )
    )


# --------------------------------------------------
# ADD DAILY EXPENSE
# --------------------------------------------------

@app.route("/expense", methods=["POST"])
def add_expense():

    customer_id = int(request.form["customer_id"])
    merchant = request.form["merchant"]
    category = request.form["category"]
    expense_date = request.form["expense_date"]

    try:
        amount = Decimal(request.form["amount"])
    except InvalidOperation:
        return "Invalid expense amount", 400

    if amount <= 0:
        return "Expense amount must be greater than zero", 400

    connection = get_connection()

    try:
        cursor = connection.cursor()

        cursor.execute("""
            INSERT INTO expenses (
                customer_id,
                merchant,
                amount,
                category,
                expense_date
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s
            );
        """, (
            customer_id,
            merchant,
            amount,
            category,
            expense_date
        ))

        connection.commit()

        cursor.close()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

    return redirect(
        url_for(
            "customer_dashboard",
            customer_id=customer_id,
            message=f"Expense of ${amount:.2f} added successfully!"
        )
    )


# --------------------------------------------------
# START FLASK
# --------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True)