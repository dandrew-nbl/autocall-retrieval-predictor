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


def get_se_db_connection():
    """Create database connection"""
    # Create the SQLAlchemy engine (FS DB)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={se_params}")
    return engine

def get_fs_db_connection():
    """Create database connection"""
    # Create the SQLAlchemy engine (FS DB)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={fs_params}")
    return engine

def load_retrieval_data():
    # Create the SQLAlchemy engine (SE DB)
    engine = get_se_db_connection()
    # Try to connect and execute a simple query
    try:
        with engine.connect() as connection:

            # Write SQL Query here
            # Your first query should be to find the minimum stageddatetime of your shipment. In other words, what time the 1st pallet was loaded.

            query = text("""
                        
                            SELECT DISTINCT
                                    [WHSE] AS ORG
                                    --,CONCAT(WHSE, RIGHT(TO_LOCATION,1)) AS Line
                              ,CASE
                                WHEN TO_LOCATION LIKE '%L1.HOFF.01%' AND WHSE = 'ALA' THEN 'ALA1'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.01%' AND WHSE = 'ATL' THEN 'ATL1'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.02%' AND WHSE = 'ATL' THEN 'ATL2'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.03%' AND WHSE = 'ATL' THEN 'ATL3'
                                WHEN TO_LOCATION LIKE '%L1.HOFF.01%' AND WHSE = 'BAK' THEN 'BAK1'
                                WHEN TO_LOCATION LIKE '%L1.HOFF.02%' AND WHSE = 'BAK' THEN 'BAK1'
                                WHEN TO_LOCATION LIKE '%L2.HOFF.01%' AND WHSE = 'BAK' THEN 'BAK2'
                                WHEN TO_LOCATION LIKE '%L2.HOFF.02%' AND WHSE = 'BAK' THEN 'BAK2'
                                WHEN TO_LOCATION LIKE '%L1.HOFF.01%' AND WHSE = 'BAY' THEN 'BAY1'
                                WHEN TO_LOCATION LIKE '%L2.HOFF.02%' AND WHSE = 'BAY' THEN 'BAY2'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.01%' AND WHSE = 'BLM' THEN 'BLM1'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.02%' AND WHSE = 'BLM' THEN 'BLM2'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.05%' AND WHSE = 'BLM' THEN 'BLM3'
                                WHEN TO_LOCATION LIKE '%L1.HNDOFF.01%' AND WHSE = 'CAR' THEN 'CAR1'
                                WHEN TO_LOCATION LIKE '%L1.HNDOFF.02%' AND WHSE = 'CAR' THEN 'CAR1'
                                WHEN TO_LOCATION LIKE '%L2.HNDOFF.01%' AND WHSE = 'CAR' THEN 'CAR2'
                                WHEN TO_LOCATION LIKE '%L2.HNDOFF.02%' AND WHSE = 'CAR' THEN 'CAR2'
                                WHEN TO_LOCATION LIKE '%L3.HNDOFF.01%' AND WHSE = 'CAR' THEN 'CAR3'
                                WHEN TO_LOCATION LIKE '%L3.HNDOFF.02%' AND WHSE = 'CAR' THEN 'CAR3'
                                WHEN TO_LOCATION LIKE '%L4.HNDOFF.01%' AND WHSE = 'CAR' THEN 'CAR4'
                                WHEN TO_LOCATION LIKE '%L4.HNDOFF.02%' AND WHSE = 'CAR' THEN 'CAR4'
                                WHEN TO_LOCATION LIKE '%L5.HNDOFF.01%' AND WHSE = 'CAR' THEN 'CAR5'
                                WHEN TO_LOCATION LIKE '%L5.HNDOFF.02%' AND WHSE = 'CAR' THEN 'CAR5'
                                WHEN TO_LOCATION LIKE '%L1.HOFF.01%' AND WHSE = 'DET' THEN 'DET1'
                                WHEN TO_LOCATION LIKE '%L2.HOFF.01%' AND WHSE = 'DET' THEN 'DET2'
                                WHEN TO_LOCATION LIKE '%L1.HOFF%' AND WHSE = 'HAZ' THEN 'HAZ1'
                                WHEN TO_LOCATION LIKE '%L2.HOFF%' AND WHSE = 'HAZ' THEN 'HAZ2'
                                WHEN TO_LOCATION LIKE '%L3.HOFF%' AND WHSE = 'HAZ' THEN 'HAZ3'
                                WHEN TO_LOCATION LIKE '%L4.HOFF%' AND WHSE = 'HAZ' THEN 'HAZ4'
                                WHEN TO_LOCATION LIKE '%L1L2.RAW.HNDOFF%' AND WHSE = 'HOU' THEN 'HOU1'
                                WHEN TO_LOCATION LIKE '%L3.RAW.HNDOFF%' AND WHSE = 'HOU' THEN 'HOU3'
                                WHEN TO_LOCATION LIKE '%L4.RAW.HNDOFF%' AND WHSE = 'HOU' THEN 'HOU4'
                                WHEN TO_LOCATION LIKE '%DDR.HOFF.L1%' AND WHSE = 'JAX' THEN 'JAX1'
                                WHEN TO_LOCATION LIKE '%DDR.HOFF.L2%' AND WHSE = 'JAX' THEN 'JAX2'
                                WHEN TO_LOCATION LIKE '%DDR.HOFF.L3%' AND WHSE = 'JAX' THEN 'JAX3'
                                WHEN TO_LOCATION LIKE '%DDR.HOFF.L4%' AND WHSE = 'JAX' THEN 'JAX4'
                                WHEN TO_LOCATION LIKE '%DDR.HOFF.L5%' AND WHSE = 'JAX' THEN 'JAX5'
                                WHEN TO_LOCATION LIKE '%HOFF.01%' AND WHSE = 'KC2' THEN 'KC21'
                                WHEN TO_LOCATION LIKE '%HOFF.02%' AND WHSE = 'KC2' THEN 'KC22'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.01%' AND WHSE = 'KEN' THEN 'KEN1'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.01%' AND WHSE = 'KEN' THEN 'KEN2'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.01%' AND WHSE = 'KEN' THEN 'KEN3'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.01%' AND WHSE = 'KEN' THEN 'KEN4'
                                WHEN TO_LOCATION LIKE '%L1.HOFF%' AND WHSE = 'KNC' THEN 'KNC1'
                                WHEN TO_LOCATION LIKE '%L2.HOFF%' AND WHSE = 'KNC' THEN 'KNC2'
                                WHEN TO_LOCATION LIKE '%L3.HOFF%' AND WHSE = 'KNC' THEN 'KNC3'
                                WHEN TO_LOCATION LIKE '%RDD.RAW.HOFF.L1%' AND WHSE = 'LOU' THEN 'LOU1'
                                WHEN TO_LOCATION LIKE '%RDD.RAW.HOFF.L2%' AND WHSE = 'LOU' THEN 'LOU2'
                                WHEN TO_LOCATION LIKE '%RDD.RAW.HOFF.L3%' AND WHSE = 'LOU' THEN 'LOU3'
                                WHEN TO_LOCATION LIKE '%RDD.RAW.HOFF.L4%' AND WHSE = 'LOU' THEN 'LOU4'
                                WHEN TO_LOCATION LIKE '%RDD.RAW.HOFF.L5%' AND WHSE = 'LOU' THEN 'LOU5'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.01%' AND WHSE = 'MES' THEN 'MES1'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.05%' AND WHSE = 'MES' THEN 'MES2'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.06%' AND WHSE = 'MES' THEN 'MES2'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.01%' AND WHSE = 'MIA' THEN 'MIA1'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.02%' AND WHSE = 'MIA' THEN 'MIA2'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.07%' AND WHSE = 'MIA' THEN 'MIA3'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.04%' AND WHSE = 'MIA' THEN 'MIA5'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.01%' AND WHSE = 'MIS' THEN 'MIS1'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.02%' AND WHSE = 'MIS' THEN 'MIS2'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.01%' AND WHSE = 'MTY' THEN 'MTY1'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.01%' AND WHSE = 'OKC' THEN 'OKC1'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.02%' AND WHSE = 'OKC' THEN 'OKC1'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.L2.01%' AND WHSE = 'OKC' THEN 'OKC2'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.L2.02%' AND WHSE = 'OKC' THEN 'OKC2'
                                WHEN TO_LOCATION LIKE '%RAW.HOFF.08%' AND WHSE = 'PIT' THEN 'PIT1'
                                WHEN TO_LOCATION LIKE '%RAW.HOFF.09%' AND WHSE = 'PIT' THEN 'PIT1'
                                WHEN TO_LOCATION LIKE '%RAW.HOFF.10%' AND WHSE = 'PIT' THEN 'PIT2'
                                WHEN TO_LOCATION LIKE '%RAW.HOFF.11%' AND WHSE = 'PIT' THEN 'PIT2'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.01%' AND WHSE = 'RCH' THEN 'RCH1'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.02%' AND WHSE = 'RCH' THEN 'RCH2'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.03%' AND WHSE = 'RCH' THEN 'RCH3'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.01%' AND WHSE = 'RIA' THEN 'RIA1'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.01%' AND WHSE = 'SAN' THEN 'SAN1'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.02%' AND WHSE = 'SAN' THEN 'SAN2'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.03%' AND WHSE = 'SAN' THEN 'SAN3'
                                WHEN TO_LOCATION LIKE '%RAW.HNDOFF.04%' AND WHSE = 'SAN' THEN 'SAN4'
                                WHEN TO_LOCATION LIKE '%L1.HOFF.01%' AND WHSE = 'ST3' THEN 'ST31'
                                WHEN TO_LOCATION LIKE '%L2.HOFF.01%' AND WHSE = 'ST3' THEN 'ST32'
                                WHEN TO_LOCATION LIKE '%L1.RAW.HNDOFF.01%' AND WHSE = 'TAC' THEN 'TAC1'
                                WHEN TO_LOCATION LIKE '%L1.RAW.HNDOFF.02%' AND WHSE = 'TAC' THEN 'TAC1'
                                WHEN TO_LOCATION LIKE '%L1.RAW.HNDOFF.03%' AND WHSE = 'TAC' THEN 'TAC1'
                                WHEN TO_LOCATION LIKE '%L3.RAW.HNDOFF.01%' AND WHSE = 'TAC' THEN 'TAC3'
                                WHEN TO_LOCATION LIKE '%L3.RAW.HNDOFF.02%' AND WHSE = 'TAC' THEN 'TAC3'
                                WHEN TO_LOCATION LIKE '%L3.RAW.HNDOFF.03%' AND WHSE = 'TAC' THEN 'TAC3'
                              END AS Line
                                    ,[SKU]
                                    ,[FROM_LOCATION]
                                    ,[TO_LOCATION]
                                    ,[INSERT_DTTM]
                                    ,[COMPLETE_DTTM]
                                    ,[TRANSPORT_ORDER_TYPE]
                                    ,DATEDIFF(MINUTE, INSERT_DTTM, COMPLETE_DTTM) AS Duration_in_Minutes
                                FROM [HISTORY].[LGV_TRANSPORT_ORDER_HISTORY]
                                WHERE 1=1
                                    AND WHSE = 'LOU'
                                    AND TRANSPORT_ORDER_TYPE LIKE '%from%Mes%'
                                    AND DATEDIFF(DAY,COMPLETE_DTTM,GETDATE()) <= 180           
                        
                        """)

            result = connection.execute(query)
            print(result)
            lou_mes_retrieval_requests = pd.DataFrame(result.fetchall(), columns=result.keys())
            #print(lou_mes_retrieval_requests)
            df = lou_mes_retrieval_requests

            # EDA to clean data because dataframe comes out of SQL quite raw

            #Convert column names to lowercase and replace spaces with underscore
            df.columns = df.columns.str.lower().str.replace(' ', '_')

            # Convert insert_dttm and complete_dttm from object to datetime
            df["insert_dttm"] = pd.to_datetime(df["insert_dttm"])
            df["complete_dttm"] = pd.to_datetime(df["complete_dttm"])

            # Create two new columns by converting insert_dttm & complete_dttm from object to another object which excludes timestamp
            df["insert_date"] = pd.to_datetime(df["insert_dttm"]).dt.date
            df["complete_date"] = pd.to_datetime(df["complete_dttm"]).dt.date

            #Convert target variable from int to float
            df["duration_in_minutes"] = df["duration_in_minutes"].astype(float)

    except Exception as e:
        print(f"Connection failed. Error: {str(e)}")

    return df

def load_production_data():
    # Create the SQLAlchemy engine (FS DB)
    engine = get_fs_db_connection()
    
    try:
        with engine.connect() as connection:

            # Write SQL Query here
            query = text("""
                        
                        SELECT ORG,CONCAT(ORG,RIGHT(LINE,1)) COLLATE DATABASE_DEFAULT AS Prod_Line, CONVERT(DATE, ACTUAL_START_TIME) AS Date, SUM(ACTUAL_QUANTITY) AS Total_Cases_Produced

                        FROM history.LMS_Workbench
                        WHERE ORG = 'LOU'
                        AND LINE NOT LIKE 'INJ%'
                        AND DATEDIFF(DAY,ACTUAL_START_TIME, GETDATE()) <= 180

                        GROUP BY ORG,CONCAT(ORG,RIGHT(LINE,1)) COLLATE DATABASE_DEFAULT, LINE, CONVERT(DATE, ACTUAL_START_TIME)
                        ORDER BY Date
            
                        """)

            result = connection.execute(query)
            print(result)
            lou_total_cases_produced = pd.DataFrame(result.fetchall(), columns=result.keys())
            print(lou_total_cases_produced)
            df = lou_total_cases_produced

    except Exception as e:
        print(f"Connection failed. Error: {str(e)}")

    return df

def load_shipping_data():
    # Create the SQLAlchemy engine (FS DB)
    engine = get_fs_db_connection()
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
            df = lou_total_cases_shipped

    except Exception as e:
        print(f"Connection failed. Error: {str(e)}")
    return df

def load_daily_jobs_data():
    # Create the SQLAlchemy engine (FS DB)
    engine = get_fs_db_connection()
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
                AND LINE NOT LIKE 'INJ%'
                AND DATEDIFF(DAY,ACTUAL_START_TIME, GETDATE()) <= 180
                AND Job_Status = 'Finished'
                GROUP BY ORG, LINE, CONVERT(DATE,ACTUAL_START_TIME)

        """
                        )

            result = connection.execute(query)
            print(result)
            lou_total_prod_jobs_per_day = pd.DataFrame(result.fetchall(), columns=result.keys())
            df = lou_total_prod_jobs_per_day
    
    except Exception as e:
        print(f"Connection failed. Error: {str(e)}")

    return df

df_for_test = load_production_data()
print(list(df_for_test.columns))