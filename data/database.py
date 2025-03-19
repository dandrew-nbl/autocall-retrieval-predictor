import pandas as pd
import sqlalchemy as sa
import os
from dotenv import load_dotenv

from sqlalchemy import create_engine, text
import urllib.parse

# Load environment variables
load_dotenv()

# Connection parameters for factory systems database
fs_server = os.getenv('FACTORY_SYSTEMS_DB_SERVER')
fs_database = os.getenv('FACTORY_SYSTEMS_DB_NAME')
fs_username = os.getenv('FACTORY_SYSTEMS_DB_USER')
fs_password = os.getenv('FACTORY_SYSTEMS_DB_PASSWORD')

# Connection parameters for Systems Engineering database
se_server = os.getenv('SE_DB_SERVER')
se_database = os.getenv('SE_DB_NAME')
se_username = os.getenv('SE_DB_USER')
se_password = os.getenv('SE_DB_PASSWORD')


# Create a connection string
fs_params = urllib.parse.quote_plus(
    f'DRIVER={{{os.getenv("DB_DRIVER")}}};'
    f'SERVER={fs_server};'
    f'DATABASE={fs_database};'
    f'UID={fs_username};'
    f'PWD={fs_password}'
)

# Create a connection string
se_params = urllib.parse.quote_plus(
    f'DRIVER={{{os.getenv("DB_DRIVER")}}};'
    f'SERVER={se_server};'
    f'DATABASE={se_database};'
    f'UID={se_username};'
    f'PWD={se_password}'
)

# Create the SQLAlchemy engine (SE DB)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={se_params}")
# Try to connect and execute a simple query
try:
    with engine.connect() as connection:

        # Write SQL Query here
        # Your first query should be to find the minimum stageddatetime of your shipment. In other words, what time the 1st pallet was loaded.

        query = text("""
                     
        SELECT
            [WHSE] AS ORG
            ,CONCAT(WHSE, RIGHT(TO_LOCATION,1)) AS LINE
            ,[SKU]
            ,[FROM_LOCATION]
            ,[TO_LOCATION]
            ,[INSERT_DTTM]
            ,[COMPLETE_DTTM]
            ,[TRANSPORT_ORDER_TYPE]
            ,DATEDIFF(MINUTE, INSERT_DTTM, COMPLETE_DTTM) AS Duration_in_Minutes
        FROM [SE-DB].[HISTORY].[LGV_TRANSPORT_ORDER_HISTORY]
        WHERE 1=1
            AND WHSE = 'LOU'
            AND TRANSPORT_ORDER_TYPE LIKE '%from%Mes%'
            AND DATEDIFF(DAY,COMPLETE_DTTM,GETDATE()) <= 180            
                     
                     """)

        result = connection.execute(query)
        print(result)
        lou_mes_retrieval_requests = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(lou_mes_retrieval_requests)

except Exception as e:
    print(f"Connection failed. Error: {str(e)}")

# Create the SQLAlchemy engine (FS DB)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={fs_params}")
# Try to connect and execute a simple query
try:
    with engine.connect() as connection:

        # Write SQL Query here
        query = text("""
                     
        WITH CTE AS (
        SELECT DISTINCT ORG, TC_SHIPMENT_ID, SHIPPED_QTY, CONVERT(DATE, ACTUAL_SHIPPED_DTTM) AS SHIPPED_DATE
        FROM history.MA_ShipmentHistory
        WHERE ORG = 'LOU'
        AND DATEDIFF(DAY,ACTUAL_SHIPPED_DTTM,GETDATE()) <= 180
        )
        ,
        CTE2 AS (
        SELECT ORG, SHIPPED_DATE, SUM(SHIPPED_QTY) AS Total_Cases_Shipped
        FROM CTE
        GROUP BY ORG, SHIPPED_DATE
        )

        SELECT ORG, SHIPPED_DATE, CAST(Total_Cases_Shipped AS int) AS Total_Cases_Shipped FROM CTE2
        ORDER BY SHIPPED_DATE
         
                     
                     """)

        result = connection.execute(query)
        print(result)
        lou_total_cases_shipped = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(lou_total_cases_shipped)

except Exception as e:
    print(f"Connection failed. Error: {str(e)}")

# Create the SQLAlchemy engine (FS DB)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={fs_params}")
# Try to connect and execute a simple query
try:
    with engine.connect() as connection:

        # Write SQL Query here
        query = text("""
                     
        SELECT ORG,CONCAT(ORG,RIGHT(LINE,1)) COLLATE DATABASE_DEFAULT AS Prod_Line, CONVERT(DATE, ACTUAL_START_TIME) AS Date, SUM(ACTUAL_QUANTITY) AS Total_Cases_Produced

        FROM history.LMS_Workbench
        WHERE ORG = 'LOU'
        AND LINE IN ('LU1', 'LU2', 'LU3', 'LU4', 'LU5')
        AND DATEDIFF(DAY,ACTUAL_START_TIME, GETDATE()) <= 180

        GROUP BY ORG,CONCAT(ORG,RIGHT(LINE,1)) COLLATE DATABASE_DEFAULT, LINE, CONVERT(DATE, ACTUAL_START_TIME)
        ORDER BY Date
         
         
                     """)

        result = connection.execute(query)
        print(result)
        lou_total_cases_produced = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(lou_total_cases_produced)

except Exception as e:
    print(f"Connection failed. Error: {str(e)}")


# Create the SQLAlchemy engine (FS DB)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={fs_params}")
# Try to connect and execute a simple query
try:
    with engine.connect() as connection:

        # Write SQL Query here
        query = text(
                     
     """
            SELECT ORG
            ,CONCAT(ORG,RIGHT(LINE,1)) COLLATE DATABASE_DEFAULT AS LINE
            ,CONVERT(DATE,ACTUAL_START_TIME) AS JOB_START_DATE
            ,COUNT(JOB_NUMBER) AS TOTAL_JOBS_FOR_LINE_ON_THIS_DAY

            FROM history.LMS_Workbench
            WHERE ORG = 'LOU'
            AND LINE IN ('LU1', 'LU2', 'LU3', 'LU4', 'LU5')
            AND DATEDIFF(DAY,ACTUAL_START_TIME, GETDATE()) <= 180
            AND Job_Status = 'Finished'
            GROUP BY ORG, LINE, CONVERT(DATE,ACTUAL_START_TIME)

    """
                     )

        result = connection.execute(query)
        print(result)
        lou_total_prod_jobs_per_day = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(lou_total_prod_jobs_per_day)

        print(lou_mes_retrieval_requests.dtypes)
        print(lou_total_cases_produced.dtypes)
        print(lou_total_cases_shipped.dtypes)
        print(lou_total_prod_jobs_per_day.dtypes)
        

except Exception as e:
    print(f"Connection failed. Error: {str(e)}")




# def get_db_connection():
#     """Create database connection"""
#     engine = create_connection_string()
#     return engine

# def load_retrieval_data():
#     """Load retrieval data from SQL database"""
#     conn = get_db_connection()
#     query = """
#     SELECT
#       (1+1)
#     """
#     df = pd.read_sql(query, conn)
#     return df

# def load_production_data():
#     """Load production data from SQL database"""
#     conn = get_db_connection()
#     query = """
#     SELECT
#         date, prod_line, total_cases_produced
#     FROM 
#         production_data
#     """
#     return pd.read_sql(query, conn)

# def load_shipping_data():
#     """Load shipping data from SQL database"""
#     conn = get_db_connection()
#     query = """
#     SELECT
#         shipped_date, total_cases_shipped
#     FROM 
#         shipping_data
#     """
#     return pd.read_sql(query, conn)
