INSERT INTO customers (
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    city,
    state
)
VALUES
(1001, 'John', 'Smith', 'john.smith@email.com', '6305551234', '1993-05-16', 'Plainfield', 'IL'),
(1002, 'Sarah', 'Miller', 'sarah.miller@email.com', '3125555678', '1988-11-02', 'Chicago', 'IL');

INSERT INTO accounts (
    account_id,
    customer_id,
    account_type,
    balance
)
VALUES
(5001, 1001, 'CHECKING', 4820.54),
(5002, 1001, 'SAVINGS', 12550.00),
(5003, 1002, 'CHECKING', 3100.25);

INSERT INTO transactions (
    transaction_id,
    account_id,
    merchant_name,
    amount,
    transaction_type,
    status,
    transaction_time
)
VALUES
(90001, 5001, 'Starbucks', 6.82, 'CARD', 'COMPLETED', '2026-08-17 08:42:15'),
(90002, 5001, 'Payroll', 3250.00, 'ACH', 'COMPLETED', '2026-08-17 09:00:00'),
(90003, 5002, 'Internal Transfer', 500.00, 'TRANSFER', 'COMPLETED', '2026-08-17 10:15:00'),
(90004, 5003, 'Walmart', 84.36, 'CARD', 'COMPLETED', '2026-08-17 14:10:22');