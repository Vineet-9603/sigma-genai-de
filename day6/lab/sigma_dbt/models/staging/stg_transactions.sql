<<<<<<< HEAD
-- models/staging/stg_fact_transactions.sql

WITH raw_transactions AS (
    SELECT
        transaction_id,
        amount,
        status,
        merchant_id,
        customer_id,
        transaction_date,
        payment_method
    FROM
        {{ source('sigma_analytics', 'fact_transactions') }}
),

cleaned_transactions AS (
    SELECT
        transaction_id,
        CAST(amount AS DECIMAL(10,2)) AS amount,
        status,
        merchant_id,
        customer_id,
        CAST(transaction_date AS DATE) AS transaction_date,
        payment_method,
        CURRENT_TIMESTAMP AS loaded_at
    FROM
        raw_transactions
    WHERE
=======
WITH cleaned_transactions AS (
    SELECT
        LOWER(transaction_id) AS transaction_id,
        CAST(amount AS DECIMAL(10, 2)) AS amount,
        LOWER(status) AS status,
        LOWER(merchant_id) AS merchant_id,
        LOWER(customer_id) AS customer_id,
        CAST(transaction_date AS DATE) AS transaction_date,
        LOWER(payment_method) AS payment_method,
        CURRENT_TIMESTAMP AS loaded_at
    FROM 
        {{ source('sigma_analytics', 'fact_transactions') }}
    WHERE 
>>>>>>> upstream/main
        merchant_id NOT LIKE 'TEST_%'
)

SELECT * FROM cleaned_transactions
```

```yaml
<<<<<<< HEAD
# models/staging/schema.yml

=======
>>>>>>> upstream/main
version: 2

models:
  - name: stg_fact_transactions
<<<<<<< HEAD
    description: "Staging model for fact_transactions. Cleans and prepares data for further transformation."
=======
    description: "Staging model for fact_transactions table. Cleans and prepares data for further transformation."
>>>>>>> upstream/main
    columns:
      - name: transaction_id
        description: "Unique identifier for each transaction."
        tests:
          - not_null
          - unique
      - name: amount
        description: "Amount of the transaction in USD."
        tests:
          - not_null
      - name: status
        description: "Status of the transaction (COMPLETED, FAILED, PENDING)."
        tests:
          - not_null
          - accepted_values:
<<<<<<< HEAD
              values: ["COMPLETED", "FAILED", "PENDING"]
=======
              values: ['completed', 'failed', 'pending']
>>>>>>> upstream/main
      - name: merchant_id
        description: "Foreign key referencing dim_merchant."
        tests:
          - not_null
<<<<<<< HEAD
=======
          - relationships:
              to: ref('dim_merchant')
              field: merchant_id
>>>>>>> upstream/main
      - name: customer_id
        description: "Foreign key referencing dim_customer."
        tests:
          - not_null
<<<<<<< HEAD
=======
          - relationships:
              to: ref('dim_customer')
              field: customer_id
>>>>>>> upstream/main
      - name: transaction_date
        description: "Date of the transaction."
        tests:
          - not_null
      - name: payment_method
        description: "Payment method used for the transaction (CREDIT_CARD, DEBIT_CARD, UPI)."
        tests:
          - not_null
          - accepted_values:
<<<<<<< HEAD
              values: ["CREDIT_CARD", "DEBIT_CARD", "UPI"]
      - name: loaded_at
        description: "Timestamp when the data was loaded into the staging table."
=======
              values: ['credit_card', 'debit_card', 'upi']
      - name: loaded_at
        description: "Timestamp when the data was loaded."
>>>>>>> upstream/main
        tests:
          - not_null
