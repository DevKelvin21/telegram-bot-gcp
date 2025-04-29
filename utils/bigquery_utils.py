from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
import uuid
import os

# Load environment variables
BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")

def log_to_bigquery(log_entry: dict):
    client = bigquery.Client()
    log_table_id = f"{BQ_PROJECT}.{BQ_DATASET}.audit_logs"
    errors = client.insert_rows_json(log_table_id, [log_entry])
    if errors:
        print(f"Audit log insert errors: {errors}")


def safe_delete(transaction_id: str):
    client = bigquery.Client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    query = f"""
    SELECT *
    FROM `{table_id}`
    WHERE transaction_id = @transaction_id
      AND operation IS NULL
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("transaction_id", "STRING", transaction_id)
        ]
    )
    result = client.query(query, job_config=job_config).result()
    result_list = list(result)
    if not result_list:
        raise ValueError(f"No matching rows found for transaction_id: {transaction_id}")
    original = result_list[0]

    shadow = dict(original)
    shadow["operation"] = "deleted"
    shadow["is_deleted"] = True
    shadow["date"] = datetime.now(timezone(timedelta(hours=-6))).strftime("%Y-%m-%d")
    insert_to_bigquery(shadow)


def safe_edit(transaction_id: str, new_data: dict):
    safe_delete(transaction_id)

    new_data.setdefault("date", datetime.now(timezone(timedelta(hours=-6))).strftime("%Y-%m-%d"))
    new_data["transaction_id"] = transaction_id
    new_data["operation"] = None
    new_data["is_deleted"] = False
    insert_to_bigquery(new_data)


def insert_to_bigquery(row: dict):
    client = bigquery.Client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    row.setdefault("transaction_id", str(uuid.uuid4()))
    errors = client.insert_rows_json(table_id, [row])
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")
    

def get_last_transaction_id():
    client = bigquery.Client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    query = f"""
    SELECT transaction_id
    FROM `{table_id}`
    WHERE operation IS NULL
      AND is_deleted = FALSE
    ORDER BY date DESC
    LIMIT 1
    """
    result = client.query(query).result()
    return list(result)[0].transaction_id if result.total_rows > 0 else None

def get_transaction_by_id(transaction_id: str):
    client = bigquery.Client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    query = f"""
    SELECT *
    FROM `{table_id}`
    WHERE transaction_id = @transaction_id
      AND operation IS NULL
      AND is_deleted = FALSE
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("transaction_id", "STRING", transaction_id)
        ]
    )
    result = client.query(query, job_config=job_config).result()
    return list(result)[0] if result.total_rows > 0 else None


def get_closure_report_by_date(date: str):
    client = bigquery.Client()
    query = f"""
        WITH latest_transactions AS (
        SELECT *
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE operation IS NULL
        )

        , ventas_efectivo AS (
        SELECT
            SUM(total_sale_price) AS efectivo_sales
        FROM latest_transactions
        WHERE payment_method = 'cash'
            AND date = '{date}'
        )

        , ventas_transferencia AS (
        SELECT
            SUM(total_sale_price) AS transfer_sales
        FROM latest_transactions
        WHERE payment_method = 'bank_transfer'
            AND date = '{date}'
        )

        , gastos_totales AS (
        SELECT
            SUM(expense.amount) AS total_expenses
        FROM latest_transactions,
        UNNEST(expenses) AS expense
        WHERE date = '{date}'
        )

        SELECT
        (SELECT efectivo_sales FROM ventas_efectivo) AS efectivo_sales,
        (SELECT transfer_sales FROM ventas_transferencia) AS transfer_sales,
        (SELECT total_expenses FROM gastos_totales) AS total_expenses
    """
    result = client.query(query).result()
    if result.total_rows > 0:
        return list(result)[0]
    else:
        return None
