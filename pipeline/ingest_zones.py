import os
import sys
import pandas as pd
from sqlalchemy import create_engine
import click
from tqdm.auto import tqdm

# -------------------------
# Core ingestion logic
# -------------------------
def run(pg_user, pg_password, pg_host, pg_port, pg_db, chunksize, target_table, data_url):
    # Validate chunksize
    try:
        chunksize = int(chunksize)
        if chunksize < 1:
            raise ValueError()
    except Exception:
        print(f"ERROR: Invalid chunksize {chunksize}. Must be an integer >= 1")
        sys.exit(1)

    # Validate DATA_URL
    if not data_url:
        print("ERROR: DATA_URL is required")
        sys.exit(1)

    print("Starting ingestion with settings:")
    print(f"  DATA_URL     = {data_url}")
    print(f"  DATABASE_URL = postgresql://{pg_user}@{pg_host}:{pg_port}/{pg_db}")
    print(f"  TABLE        = {target_table}")
    print(f"  CHUNKSIZE    = {chunksize}")

    # Connect to PostgreSQL
    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    # Read CSV in chunks
    try:
        df_iter = pd.read_csv(
            data_url,
            iterator=True,
            chunksize=chunksize
        )
    except Exception as e:
        print(f"ERROR reading CSV: {e}")
        sys.exit(1)

    first = True
    for i, df_chunk in enumerate(df_iter, start=1):
        start_time = pd.Timestamp.now()
        if first:
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists='replace', index=False)
            first = False

        df_chunk.to_sql(name=target_table, con=engine, if_exists='append', index=False)
        end_time = pd.Timestamp.now()
        print(f"Chunk {i} loaded in {(end_time - start_time).total_seconds():.2f} seconds")

    print("Ingestion completed successfully!")


# -------------------------
# CLI
# -------------------------
@click.command()
@click.option('--pg_user', default=lambda: os.environ.get('PG_USER', 'postgres'), help='PostgreSQL user')
@click.option('--pg_password', default=lambda: os.environ.get('PG_PASSWORD', 'postgres'), help='PostgreSQL password')
@click.option('--pg_host', default=lambda: os.environ.get('PG_HOST', 'localhost'), help='PostgreSQL host')
@click.option('--pg_port', default=lambda: int(os.environ.get('PG_PORT', 5433)), type=int, help='PostgreSQL port')
@click.option('--pg_db', default=lambda: os.environ.get('PG_DB', 'ny_taxi'), help='PostgreSQL database name')
@click.option('--chunksize', default=lambda: int(os.environ.get('CHUNKSIZE', 100000)), help='Chunk size for ingestion')
@click.option('--target_table', default=lambda: os.environ.get('TARGET_TABLE', 'taxi_zone_lookup'), help='Target table name')
@click.option('--data_url', default=lambda: os.environ.get(
    'DATA_URL', 
    'https://raw.githubusercontent.com/patrickDev/docker-workshop-Module1/refs/heads/main/pipeline/assests/taxi_zone_lookup.csv'
), help='CSV file URL')
def main(pg_user, pg_password, pg_host, pg_port, pg_db, chunksize, target_table, data_url):
    run(pg_user, pg_password, pg_host, pg_port, pg_db, chunksize, target_table, data_url)


# -------------------------
# Entry point
# -------------------------
if __name__ == "__main__":
    main()
