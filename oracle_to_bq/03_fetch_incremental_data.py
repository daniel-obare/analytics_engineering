# fetch_incremental_data.py
import oracledb
import pandas as pd
from datetime import datetime
from secrets import ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN

def fetch_incremental_data(last_load_date):
    """
    Fetch incremental data from Oracle DB based on last_updated date.
    Adds a dynamic id_date column in YYYYMMDD format.
    """
    with oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN) as conn:
        query = f"""
        SELECT *
        FROM contacts
        WHERE last_updated > TO_DATE('{last_load_date.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')
        """
        df = pd.read_sql(query, conn)
        
        # Add dynamic id_date column in YYYYMMDD format
        today_str = datetime.now().strftime('%Y%m%d')
        df['id_date'] = today_str
        
        return df