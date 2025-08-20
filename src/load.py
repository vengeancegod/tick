import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()

def load_csv_to_postgres():
    
    db_url = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('POSTGRES_DB')}"
    
    csv_file = 'data/20241001_fut_ord.csv'
    
    df = pd.read_csv(csv_file, header=0, skiprows=1, names=['symbol', 'system', 'type', 'moment', 'id', 'action', 'price', 'volume', 'id_deal', 'price_deal'],  nrows=10000)
    df = df.fillna(0)

    df['operation'] = df['type'].str.strip()
    df['order_type'] = df['action'].apply(lambda x: 0 if x == '0' else (2 if x == '2' else 1))
    df['timestamp'] = pd.to_numeric(df['moment'], errors='coerce').fillna(0).astype(int)
    
    result_df = df[[
        'symbol', 'operation', 'timestamp', 'id', 
        'order_type', 'price', 'volume'
    ]].rename(columns={
        'symbol': 'instrument',
        'id': 'order_id'
    })

    engine = create_engine(db_url)
    result_df.to_sql(
        'staging_orders',
        engine,
        if_exists='append',
        index=False,
    )
    print(f"Данные агружены")
    

if __name__ == "__main__":
    load_csv_to_postgres()