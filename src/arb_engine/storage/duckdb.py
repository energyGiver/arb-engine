"""DuckDB connection and query management."""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd

from arb_engine.core.errors import StorageError
from arb_engine.core.logging import get_logger
from arb_engine.domain.schema import get_all_table_schemas

logger = get_logger(__name__)


class DuckDBConnection:
    """
    Manage DuckDB connection and operations.

    DuckDB is an embedded analytical database, perfect for local storage
    with high performance on analytical queries.
    """

    def __init__(self, db_path: str = "data/duckdb/arb_engine.db"):
        """
        Initialize DuckDB connection.

        Args:
            db_path: Path to DuckDB database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self._connect()
        self._initialize_schema()

    def _connect(self) -> None:
        """Establish connection to DuckDB."""
        try:
            self.conn = duckdb.connect(str(self.db_path))
            logger.info("duckdb_connected", db_path=str(self.db_path))
        except Exception as e:
            logger.error("duckdb_connection_failed", error=str(e))
            raise StorageError(f"Failed to connect to DuckDB: {e}")

    def _initialize_schema(self) -> None:
        """Create tables if they don't exist."""
        if not self.conn:
            raise StorageError("No active DuckDB connection")

        try:
            schemas = get_all_table_schemas()
            for schema_ddl in schemas:
                self.conn.execute(schema_ddl)
            logger.info("duckdb_schema_initialized")
        except Exception as e:
            logger.error("schema_initialization_failed", error=str(e))
            raise StorageError(f"Failed to initialize schema: {e}")

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute a SQL query.

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            Query result
        """
        if not self.conn:
            raise StorageError("No active DuckDB connection")

        try:
            if params:
                result = self.conn.execute(query, params)
            else:
                result = self.conn.execute(query)
            return result
        except Exception as e:
            logger.error("query_execution_failed", query=query[:100], error=str(e))
            raise StorageError(f"Query execution failed: {e}")

    def query_df(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Execute query and return results as DataFrame.

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            Query results as pandas DataFrame
        """
        result = self.execute(query, params)
        return result.df()

    def insert_batch(
        self, table: str, data: List[Dict[str, Any]], replace: bool = False
    ) -> None:
        """
        Insert batch of records into table.

        Args:
            table: Table name
            data: List of records as dictionaries
            replace: If True, replace existing records (based on primary key)
        """
        if not data:
            return

        if not self.conn:
            raise StorageError("No active DuckDB connection")

        try:
            df = pd.DataFrame(data)
            if replace:
                # Use REPLACE INTO equivalent (INSERT OR REPLACE)
                self.conn.execute(f"INSERT OR REPLACE INTO {table} SELECT * FROM df")
            else:
                self.conn.execute(f"INSERT INTO {table} SELECT * FROM df")
            logger.debug(
                "batch_inserted", table=table, row_count=len(data), replace=replace
            )
        except Exception as e:
            logger.error("batch_insert_failed", table=table, error=str(e))
            raise StorageError(f"Batch insert failed: {e}")

    def insert_one(self, table: str, data: Dict[str, Any]) -> None:
        """
        Insert single record into table.

        Args:
            table: Table name
            data: Record as dictionary
        """
        self.insert_batch(table, [data])

    def export_to_parquet(
        self, query: str, output_path: Path, partition_by: Optional[List[str]] = None
    ) -> None:
        """
        Export query results to Parquet file.

        Args:
            query: SQL query
            output_path: Output Parquet file path
            partition_by: Optional list of columns to partition by
        """
        if not self.conn:
            raise StorageError("No active DuckDB connection")

        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)

            if partition_by:
                partition_clause = ", ".join(partition_by)
                export_query = f"""
                    COPY ({query})
                    TO '{output_path}'
                    (FORMAT PARQUET, PARTITION_BY ({partition_clause}))
                """
            else:
                export_query = f"COPY ({query}) TO '{output_path}' (FORMAT PARQUET)"

            self.conn.execute(export_query)
            logger.info("exported_to_parquet", output_path=str(output_path))
        except Exception as e:
            logger.error("parquet_export_failed", error=str(e))
            raise StorageError(f"Parquet export failed: {e}")

    def load_from_parquet(self, table: str, parquet_path: Path) -> None:
        """
        Load data from Parquet file into table.

        Args:
            table: Table name
            parquet_path: Path to Parquet file or directory
        """
        if not self.conn:
            raise StorageError("No active DuckDB connection")

        try:
            self.conn.execute(f"INSERT INTO {table} SELECT * FROM '{parquet_path}'")
            logger.info("loaded_from_parquet", table=table, parquet_path=str(parquet_path))
        except Exception as e:
            logger.error("parquet_load_failed", error=str(e))
            raise StorageError(f"Parquet load failed: {e}")

    def close(self) -> None:
        """Close DuckDB connection."""
        if self.conn:
            self.conn.close()
            logger.info("duckdb_closed")
            self.conn = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
