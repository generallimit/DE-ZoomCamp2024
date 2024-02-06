#!/usr/bin/env python
# coding: utf-8



import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time


def main(params):
    user=params.user
    pwd=params.pwd
    host=params.host
    port=params.port
    db=params.db
    tbl_name=params.tbl_name
    url=params.url

    csv_name='output.csv'

    # Download the CSV
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{pwd}@{host}:{port}/{db}')

    df_iter = pd.read_csv(url, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=tbl_name, con=engine, if_exists='replace')

    while True: 
        t_start = time()

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=tbl_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))



if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                        prog='ProgramName',
                        description='Ingest CSV data to PostgreSQL',
                        epilog='Text at the bottom of help')

    parser.add_argument('--user', help='username for postgresql') 
    parser.add_argument('--pwd', help='password for postgresql')  
    parser.add_argument('--host', help='host name for postgresql')  
    parser.add_argument('--port', help='port for postgresql')  
    parser.add_argument('--db', help='database name for postgresql')  
    parser.add_argument('--tbl_name', help='table name where results are written')  
    parser.add_argument('--url', help='url of the csv file') 

    args = parser.parse_args()

    main(args)
