#!/usr/bin/env python
# coding: utf-8



import os
import argparse
import pandas as pd
import pyarrow as pa
import glob2 as glob
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

    pqt_name='output.pqt'
    # url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
    # Download the CSV
    os.system(f"wget {url} -O {pqt_name} --no-check-certificate")

    filenames = glob.glob('*.pqt')
    def parq():
      for f in filenames:
        print(filenames)
    pqt_name="','".join(filenames)
    # print(pqt_name)
    # engine = create_engine(f'postgresql://root:root@pgdatabase:5432/ny_taxi')
    engine = create_engine(f'postgresql://{user}:{pwd}@{host}:{port}/{db}')

    df = pd.read_parquet(pqt_name, engine='pyarrow')

    while True: 
        t_start = time()

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=tbl_name, con=engine, if_exists='append')

        t_end = time()

        print('insertion took %.3f second' % (t_end - t_start))



if __name__ == '__main__':

    parser = argparse.ArgumentParser(
                        prog='ProgramName',
                        description='Ingest data to PostgreSQL',
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