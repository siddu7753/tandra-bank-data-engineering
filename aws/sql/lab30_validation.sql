
-- Tandra Bank - Lab 30 Athena Validation
-- Database: tandra_bank_raw
-- These queries use existing Athena external tables.

-- 1. Verify processed transaction totals
SELECT
    COUNT(*) AS total_processed_records,
    COUNT(DISTINCT transaction_id) AS unique_transactions,
    SUM(amount) AS total_transaction_amount
FROM tandra_bank_raw.lab30_processed_transactions;

-- 2. Verify original historical transaction
SELECT
    transaction_id,
    account_id,
    amount,
    merchant
FROM tandra_bank_raw.lab30_processed_transactions
WHERE transaction_id = 'TXN30002';

-- 3. Verify historical conflict in quarantine
SELECT
    transaction_id,
    account_id,
    amount,
    merchant,
    rejection_reason
FROM tandra_bank_raw.lab30_quarantine_transactions
WHERE transaction_id = 'TXN30002';

-- 4. Summarize quarantine rejection reasons
SELECT
    rejection_reason,
    COUNT(*) AS rejected_records
FROM tandra_bank_raw.lab30_quarantine_transactions
GROUP BY rejection_reason
ORDER BY rejected_records DESC;
