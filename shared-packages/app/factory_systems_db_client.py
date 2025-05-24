from typing import Dict, Any, List, Optional, Union
import requests
import pandas as pd
import logging
import os

class DatabaseError(Exception):
    def __init__(self, message, error_type=None):
        super().__init__(message)
        self.error_type = error_type

class QueryTimeoutError(DatabaseError):
    pass

class InvalidColumnError(DatabaseError):
    pass

class InvalidTableError(DatabaseError):
    pass

class FactorySystemsDB:
    def __init__(self, flow_url: str = None):
        self.flow_url = (
            flow_url or
            os.getenv('FACTORY_DB_FLOW_URL')
        )

        if not self.flow_url:
            raise ValueError(
                "   No flow URL provided. Please set it via:\n"
                "1. The flow_url argument when instantiating FactorySystemsDB, or\n"
                "2. The FACTORY_DB_FLOW_URL environment variable (recommended for Docker)"
        )

        if not self.flow_url.startswith("http"):
            raise ValueError("Invalid Power Automate Flow URL.")


        self.headers = {'Content-Type': 'application/json'}
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            logging.basicConfig(level=logging.INFO)
        self.timeout = 600

    def run_operation(
        self,
        operation: str,
        sql_or_table: str,
        params_or_records: Union[Dict[str, Any], List[Dict[str, Any]]] = None,
        database: str = "mis",
        timeout: int = None
    ) -> Any:
        payload = {
            "operation": operation,
            "database": database
        }

        if operation == "query":
            payload["query"] = sql_or_table
            payload["params"] = params_or_records or {}

        elif operation == "insert":
            payload["command"] = sql_or_table
            payload["params"] = params_or_records or {}

        elif operation == "bulk":
            payload["table"] = sql_or_table
            payload["records"] = params_or_records or []

        else:
            raise ValueError(f"Unsupported operation: {operation}")

        try:
            self.logger.info(f"Executing {operation}: {payload}")
            response = requests.post(
                self.flow_url,
                json=payload,
                headers=self.headers,
                timeout=timeout or self.timeout
            )
            if response.status_code >= 400:
                try:
                    error_data = response.json()
                    error_type = error_data.get("error_type", "UNKNOWN")
                    error_msg = error_data.get("error", "Unknown error")
                    if error_type == "INVALID_COLUMN":
                        raise InvalidColumnError(error_msg, error_type)
                    elif error_type == "INVALID_TABLE":
                        raise InvalidTableError(error_msg, error_type)
                    elif error_type == "TIMEOUT":
                        raise QueryTimeoutError(error_msg, error_type)
                    else:
                        raise DatabaseError(error_msg, error_type)
                except ValueError:
                    response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            raise QueryTimeoutError(f"{operation} timed out after {timeout or self.timeout} seconds")
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Request failed: {e}")
        except Exception as e:
            raise DatabaseError(f"Unexpected error: {e}")

    def query(self, sql: str, params: Optional[Dict[str, Any]] = None, timeout: int = None, database: str = "mis") -> List[Dict[str, Any]]:
        return self.run_operation("query", sql, params, database=database, timeout=timeout)

    def insert(self, command: str, params: Optional[Dict[str, Any]] = None, timeout: int = None, database: str = "mis") -> Dict[str, Any]:
        return self.run_operation("insert", command, params, database=database, timeout=timeout)

    def bulk_insert(self, table: str, records: List[Dict[str, Any]], timeout: int = None, database: str = "mis") -> Dict[str, Any]:
        return self.run_operation("bulk", table, records, database=database, timeout=timeout)

    def query_to_dataframe(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        data = self.query(sql, params)
        df = pd.DataFrame(data)
        for col in df.columns:
            if 'stamp' in col.lower() or 'date' in col.lower() or 'time' in col.lower():
                try:
                    df[col] = pd.to_datetime(df[col])
                except Exception:
                    pass
        return df

def connect(flow_url: str = None) -> FactorySystemsDB:
    return FactorySystemsDB(flow_url)
