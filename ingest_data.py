import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_name = 'output.parquet'
    csv_name = 'output.csv'

    # Download parquet file from source
    os.system(f"wget {url} -O {parquet_name}")
    
    # Convert parquet file to csv
    pd.read_parquet(parquet_name).to_csv(csv_name)

    # Connect to the database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    # Create DDL statement
    # pd.io.sql.get_schema(df, name=table_name, con=engine)

    # Load the large csv file as a generator in chunks of 100,000 rows
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    # Check if the table exists before inserting the column name row
    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")


    while True:
        t_start = time()
        df = next(df_iter)
        
        # Preprocess date columns for yellow taxi trip data
        #df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        #df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        # Preprocess date columns for fhv trip data
        df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
        df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)
        
        df.to_sql(name=table_name, con=engine, if_exists="append")

        t_end = time()

        print("inserted a chunk..., took %.3f seconds" % (t_end - t_start))

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data into Postgresql')
    parser.add_argument('--user', help='postgres username')
    parser.add_argument('--password', help='postgres password')
    parser.add_argument('--host', help='postgres host')
    parser.add_argument('--port', type=int, help='postgres port')
    parser.add_argument('--db', help='postgres database name')
    parser.add_argument('--table_name', help='postgres table name in the database')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)