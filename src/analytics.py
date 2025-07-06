import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv("../.env")

db_url = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DATAWAREHOUSE_NAME')}"
engine = create_engine(db_url)

def createAnalyticsTables():
    tables_to_create = {
        'tb_cars_quantity_by_state': cars_quantity_by_state,
        'tb_cars_average_price_by_state' : cars_average_price_by_state,
        'table_cars_count_by_manufacture_uf' : cars_count_by_manufacture_uf,
        'table_cars_count_by_manufatcure' : cars_count_by_manufatcure
    }

    for table_name, function in tables_to_create.items():
        df = function()
        save_df_as_table(df, table_name)

def cars_count_by_manufacture_uf():
    with open("../sql/analytics/table_cars_count_by_manufacture_uf.sql", "r") as opened_file:
        query = opened_file.read()
    df = pd.read_sql(query, engine)
    
    return df
    
def cars_count_by_manufatcure():
    with open("../sql/analytics/table_cars_count_by_manufatcure.sql", "r") as opened_file:
        query = opened_file.read()
    df = pd.read_sql(query, engine)
    
    return df
    
def cars_quantity_by_state():
    with open("../sql/analytics/table_cars_count_by_state.sql", "r") as opened_file:
        query = opened_file.read()
    df = pd.read_sql(query, engine)
    
    return df

def cars_average_price_by_state():
    with open("../sql/analytics/table_cars_average_price_by_state.sql", "r") as opened_file:
        query = opened_file.read()
        
    df = pd.read_sql(query, engine)
    
    return df

def save_df_as_table(df, table_name: str, schema = "analytics"):
      df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists='replace',  # ou 'append'
        index=False
    )
      
      