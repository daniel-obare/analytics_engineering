# loader.py
import pandas as pd
from sqlalchemy import create_engine

def load_to_database(df, conn_str, table_name='disbursement', if_exists='append'):
    engine = create_engine(conn_str)
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    print(f"Data loaded to table '{table_name}' successfully.")