import os
import pandas as pd
from sqlalchemy import create_engine
import click

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database')
@click.option('--table-name', required=True, help='Target table name')
@click.option('--url', required=True, help='URL of the parquet or csv file')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, table_name, url):
    
    # 1. Create SQL Engine
    connection_string = f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    engine = create_engine(connection_string)
    
    print(f"Downloading file from {url}...")
    
    # 2. Determine file type and read accordingly
    if url.endswith('.parquet'):
        # --- PARQUET LOGIC ---
        # Parquet files are smaller and contain schema, so we can usually read them directly
        df = pd.read_parquet(url)
        
        # Fix column names for Green Taxi (if needed) or just ensure types
        # Note: Parquet already preserves types like Int64 and datetime, 
        # so we don't need the complex dtype dictionary or parse_dates used for CSVs.
        
        print(f"Read {len(df)} rows from Parquet.")
        print(f"Writing to table '{table_name}'...")
        
        # Write to SQL
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print("Done!")

    else:
        # --- CSV LOGIC (For Lookup Table) ---
        # Fallback for the CSV lookup file
        df = pd.read_csv(url)
        
        print(f"Read {len(df)} rows from CSV.")
        print(f"Writing to table '{table_name}'...")
        
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print("Done!")

if __name__ == '__main__':
    run()