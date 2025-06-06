import io
import pandas as pd
import glob
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = [
            'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-12.csv.gz',
            'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz',
            'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz'
        ]

    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RatecodeID':pd.Int64Dtype(),
                    'store_and_fwd_flag':str,
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra':float,
                    'mta_tax':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'improvement_surcharge':float,
                    'total_amount':float,
                    'congestion_surcharge':float
                }

    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    dfs = []
    for csv_file in url:
        df = pd.read_csv(csv_file, sep=",", compression="gzip", dtype=taxi_dtypes, parse_dates=parse_dates)
        dfs.append(df)
        # pd.concat(map(pd.read_csv, csv_file), ignore_index=True, axis=0) 
        # df1 = pd.read_csv(csv_file, sep=",", compression="gzip", dtype=taxi_dtypes, parse_dates=parse_dates)
        # print(df.shape[0])
    data = pd.concat(dfs, ignore_index=True)
    
    print(f'Row count: {data.shape[0]}\nCol count: {data.shape[1]}\n')
    print(data.info())
    return data

