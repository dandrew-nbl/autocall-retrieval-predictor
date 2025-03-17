import pandas as pd
import sqlalchemy as sa

def get_db_connection():
    """Create database connection"""
    engine = sa.create_engine('postgresql://user:password@localhost:5432/warehouse_db')
    return engine

def load_retrieval_data():
    """Load retrieval data from SQL database"""
    conn = get_db_connection()
    query = """
    SELECT
        line, item, item_type, from_location, to_location,
        insert_dttm, complete_dttm, duration_in_minutes,
        insert_date, complete_date
    FROM 
        warehouse_retrievals
    WHERE 
        insert_dttm >= '2024-01-01'
    """
    df = pd.read_sql(query, conn)
    return df

def load_production_data():
    """Load production data from SQL database"""
    conn = get_db_connection()
    query = """
    SELECT
        date, prod_line, total_cases_produced
    FROM 
        production_data
    """
    return pd.read_sql(query, conn)

def load_shipping_data():
    """Load shipping data from SQL database"""
    conn = get_db_connection()
    query = """
    SELECT
        shipped_date, total_cases_shipped
    FROM 
        shipping_data
    """
    return pd.read_sql(query, conn)
