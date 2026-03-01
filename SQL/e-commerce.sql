CREATE TABLE transactions (
    transaction_id      INT PRIMARY KEY,
    transaction_date    TIMESTAMP,
    gender              VARCHAR(10),
    age                 INT,
    marital_status      VARCHAR(20),
    state_name          VARCHAR(50),
    segment             VARCHAR(20),
    employees_status    VARCHAR(30),
    payment_method      VARCHAR(20),
    referral            INT,
    amount_spent        NUMERIC(12,2)
);

SELECT COUNT(*) FROM transactions

CREATE INDEX idx_transaction_date ON transactions(transaction_date);
CREATE INDEX idx_segment ON transactions(segment);
CREATE INDEX idx_state ON transactions(state_name);
CREATE INDEX idx_payment ON transactions(payment_method);
CREATE INDEX idx_amount ON transactions(amount_spent);

-- 1) Row count
SELECT COUNT(*) AS total_rows FROM transactions;

-- 2) Null checks (important columns)
SELECT
  SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) AS null_transaction_id,
  SUM(CASE WHEN transaction_date IS NULL THEN 1 ELSE 0 END) AS null_transaction_date,
  SUM(CASE WHEN amount_spent IS NULL THEN 1 ELSE 0 END) AS null_amount_spent,
  SUM(CASE WHEN segment IS NULL THEN 1 ELSE 0 END) AS null_segment,
  SUM(CASE WHEN state_name IS NULL THEN 1 ELSE 0 END) AS null_state_name,
  SUM(CASE WHEN payment_method IS NULL THEN 1 ELSE 0 END) AS null_payment_method
FROM transactions;

-- 3) Amount sanity
SELECT
  MIN(amount_spent) AS min_amount,
  MAX(amount_spent) AS max_amount,
  AVG(amount_spent) AS avg_amount
FROM transactions;

-- 4) Quick category check
SELECT segment, COUNT(*) AS rows
FROM transactions
GROUP BY segment
ORDER BY rows DESC;

SELECT payment_method, COUNT(*) AS rows
FROM transactions
GROUP BY payment_method
ORDER BY rows DESC;

CREATE OR REPLACE VIEW v_transactions_clean AS
SELECT *
FROM transactions
WHERE amount_spent IS NOT NULL;

SELECT COUNT(*) FROM v_transactions_clean;

CREATE OR REPLACE VIEW v_monthly_revenue AS
SELECT
    DATE_TRUNC('month', transaction_date) AS month,
    COUNT(*) AS total_transactions,
    SUM(amount_spent) AS total_revenue,
    AVG(amount_spent) AS avg_order_value
FROM v_transactions_clean
GROUP BY 1
ORDER BY 1;

SELECT * FROM v_monthly_revenue
LIMIT 5;

CREATE OR REPLACE VIEW v_monthly_revenue_growth AS
SELECT
    month,
    total_transactions,
    total_revenue,
    avg_order_value,
    LAG(total_revenue) OVER (ORDER BY month) AS previous_month_revenue,
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (ORDER BY month))
        / LAG(total_revenue) OVER (ORDER BY month) * 100,
        2
    ) AS revenue_growth_pct
FROM v_monthly_revenue;

SELECT * 
FROM v_monthly_revenue_growth
LIMIT 6;

CREATE OR REPLACE VIEW v_segment_revenue AS
WITH segment_data AS (
    SELECT
        segment,
        SUM(amount_spent) AS segment_revenue
    FROM v_transactions_clean
    GROUP BY segment
),
total_data AS (
    SELECT SUM(segment_revenue) AS total_revenue
    FROM segment_data
)
SELECT
    s.segment,
    s.segment_revenue,
    ROUND(
        s.segment_revenue / t.total_revenue * 100,
        2
    ) AS revenue_contribution_pct
FROM segment_data s
CROSS JOIN total_data t
ORDER BY s.segment_revenue DESC;

SELECT * FROM v_segment_revenue;

CREATE OR REPLACE VIEW v_state_revenue AS
SELECT
    state_name,
    COUNT(*) AS transactions,
    SUM(amount_spent) AS total_revenue,
    ROUND(AVG(amount_spent), 2) AS avg_order_value
FROM v_transactions_clean
GROUP BY state_name
ORDER BY total_revenue DESC;

SELECT * 
FROM v_state_revenue
LIMIT 5;

CREATE OR REPLACE VIEW v_customer_metrics AS
SELECT
    transaction_id,
    MAX(transaction_date) OVER (PARTITION BY transaction_id) AS last_purchase_date,
    COUNT(*) OVER (PARTITION BY transaction_id) AS total_orders,
    SUM(amount_spent) OVER (PARTITION BY transaction_id) AS total_spent
FROM v_transactions_clean;

SELECT * 
FROM v_customer_metrics
LIMIT 5;

CREATE OR REPLACE VIEW v_payment_analysis AS
SELECT
    payment_method,
    COUNT(*) AS transactions,
    SUM(amount_spent) AS total_revenue,
    ROUND(AVG(amount_spent), 2) AS avg_order_value
FROM v_transactions_clean
GROUP BY payment_method
ORDER BY total_revenue DESC;

SELECT * FROM v_payment_analysis;

SELECT
    CASE 
        WHEN amount_spent < 500 THEN 'Low (<500)'
        WHEN amount_spent BETWEEN 500 AND 1500 THEN 'Medium (500-1500)'
        ELSE 'High (>1500)'
    END AS spending_tier,
    COUNT(*) AS transactions,
    SUM(amount_spent) AS total_revenue,
    ROUND(SUM(amount_spent) / 
          (SELECT SUM(amount_spent) FROM v_transactions_clean) * 100, 2) 
          AS revenue_share_pct
FROM v_transactions_clean
GROUP BY 1
ORDER BY total_revenue DESC;