from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
import uuid
import os

class BigQueryUtils:
    """
    A utility class for interacting with Google BigQuery to perform operations
    such as logging, managing transactions, and generating reports.
    """

    def __init__(self, timezone_obj):
        """
        Initializes the BigQueryUtils class with a BigQuery client instance,
        environment variables for project, dataset, and table, and a timezone object.

        Args:
            timezone_obj (timezone): A timezone object to ensure consistent timezone handling.
        """
        self.client = bigquery.Client()
        self.project = os.getenv("BQ_PROJECT")
        self.dataset = os.getenv("BQ_DATASET")
        self.table = os.getenv("BQ_TABLE")
        self.timezone = timezone_obj

    def log_to_bigquery(self, log_entry: dict):
        """
        Logs an entry to the BigQuery audit_logs table.

        Args:
            log_entry (dict): A dictionary containing the log data to be inserted.

        Raises:
            Prints errors if the insertion fails.
        """
        log_table_id = f"{self.project}.{self.dataset}.audit_logs"
        errors = self.client.insert_rows_json(log_table_id, [log_entry])
        if errors:
            print(f"Audit log insert errors: {errors}")

    def safe_delete(self, transaction_id: str):
        """
        Marks a transaction as deleted by creating a shadow entry with the
        "operation" field set to "deleted".

        Args:
            transaction_id (str): The ID of the transaction to delete.

        Raises:
            ValueError: If no matching transaction is found.
        """
        table_id = f"{self.project}.{self.dataset}.{self.table}"
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
        result = self.client.query(query, job_config=job_config).result()
        result_list = list(result)
        if not result_list:
            raise ValueError(f"No matching rows found for transaction_id: {transaction_id}")
        original = result_list[0]

        shadow = dict(original)
        shadow["operation"] = "deleted"
        shadow["is_deleted"] = True
        shadow["date"] = datetime.now(self.timezone).strftime("%Y-%m-%d")
        self.insert_to_bigquery(shadow)

    def safe_edit(self, transaction_id: str, new_data: dict):
        """
        Safely edits a transaction by first marking the original transaction
        as deleted and then inserting the updated data.

        Args:
            transaction_id (str): The ID of the transaction to edit.
            new_data (dict): A dictionary containing the updated transaction data.
        """
        self.safe_delete(transaction_id)

        new_data.setdefault("date", datetime.now(self.timezone).strftime("%Y-%m-%d"))
        new_data["transaction_id"] = transaction_id
        new_data["operation"] = None
        new_data["is_deleted"] = False
        self.insert_to_bigquery(new_data)

    def insert_to_bigquery(self, row: dict):
        """
        Inserts a new row into the BigQuery table.

        Args:
            row (dict): A dictionary containing the row data to be inserted.

        Raises:
            RuntimeError: If the insertion fails.
        """
        table_id = f"{self.project}.{self.dataset}.{self.table}"
        row.setdefault("transaction_id", str(uuid.uuid4()))
        errors = self.client.insert_rows_json(table_id, [row])
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")

    def get_last_transaction_id(self):
        """
        Retrieves the ID of the most recent transaction that is not marked as deleted.

        Returns:
            str: The ID of the last transaction, or None if no transactions exist.
        """
        table_id = f"{self.project}.{self.dataset}.{self.table}"
        query = f"""
        SELECT transaction_id
        FROM `{table_id}`
        WHERE operation IS NULL
          AND is_deleted = FALSE
        ORDER BY date DESC
        LIMIT 1
        """
        result = self.client.query(query).result()
        return list(result)[0].transaction_id if result.total_rows > 0 else None

    def get_transaction_by_id(self, transaction_id: str):
        """
        Retrieves a transaction by its ID.

        Args:
            transaction_id (str): The ID of the transaction to retrieve.

        Returns:
            dict: The transaction data, or None if no matching transaction is found.
        """
        table_id = f"{self.project}.{self.dataset}.{self.table}"
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
        result = self.client.query(query, job_config=job_config).result()
        return list(result)[0] if result.total_rows > 0 else None

    def get_closure_report_by_date(self, date: str):
        """
        Generates a closure report for a specific date, including sales and expenses.

        Args:
            date (str): The date for which to generate the report (in "YYYY-MM-DD" format).

        Returns:
            dict: A dictionary containing the closure report data, or None if no data exists.
        """
        query = f"""
            WITH latest_transactions AS (
            SELECT *
            FROM `{self.project}.{self.dataset}.{self.table}`
            ),

            unique_transactions AS (
            SELECT *
            FROM latest_transactions
            WHERE transaction_id IN (
                SELECT transaction_id
                FROM latest_transactions
                GROUP BY transaction_id
                HAVING COUNT(transaction_id) = 1
            )
            ),

            ventas_efectivo AS (
            SELECT
                SUM(total_sale_price) AS efectivo_sales
            FROM unique_transactions
            WHERE payment_method = 'cash'
                AND date = @date
            ),

            ventas_transferencia AS (
            SELECT
                SUM(total_sale_price) AS transfer_sales
            FROM unique_transactions
            WHERE payment_method = 'bank_transfer'
                AND date = @date
            ),

            gastos_totales AS (
            SELECT
                SUM(expense.amount) AS total_expenses
            FROM unique_transactions,
            UNNEST(expenses) AS expense
            WHERE date = @date
            )

            SELECT
            (SELECT efectivo_sales FROM ventas_efectivo) AS efectivo_sales,
            (SELECT transfer_sales FROM ventas_transferencia) AS transfer_sales,
            (SELECT total_expenses FROM gastos_totales) AS total_expenses
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "STRING", date)
            ]
        )
        result = self.client.query(query, job_config=job_config).result()
        if result.total_rows > 0:
            return list(result)[0]
        else:
            return None
