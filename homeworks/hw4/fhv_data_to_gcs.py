#!/usr/bin/env python
# coding: utf-8
# author: drux31 <contact@lnts.me>
# date : 2024-02-09
"""
    Program that download the taxi data parquet files
    needed for module, and load them into cloud storage
    --> define the GOOGLE_APPLICATION_CREDENTIALS env variable
"""

import os
import urllib.request
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
#import requests
from google.cloud import storage

def write_to_bucket(filename, bucket_name, service, year):
    """Write green data parquet file into the given bucket"""
    # Instasiate a client
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    
    destination_blob = bucket.blob(f'{service}/{year}/{filename}')

    destination_blob.upload_from_filename(f"data/{service}/{filename}")
    print(f"file data/{service}/{filename} uploaded to {destination_blob}")

def enforce_dtype(filename, service):
    """
    Function for cleaning the parquet file and make sure
    all the files share the same data types
    """
    fhv_dtypes = {
                    'dispatching_base_num': str,
                    'PUlocationID': pd.Int64Dtype(),
                    'DOlocationID': pd.Int64Dtype(),
                    'SR_Flag': pd.Int64Dtype()
                }
    parse_dates = ['pickup_datetime', 'dropOff_datetime']
    df = pd.read_csv(f"data/{service}/{filename}", compression='gzip', dtype=fhv_dtypes, parse_dates=parse_dates)
    return df


def main(service, year):
    """Main for the program"""
    # Name of the bucket 
    bucket_name = 'de-zoomcamp-bucket-drux'
    #folder name
    #semi_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
    # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz
    
    months = ['01', '02', '03', '04', '05', '06', 
              '07', '08', '09', '10', '11', '12']
    #years = ['2019', '2020']
    #folder_names = ['green_tripdata', 'yellow_tripdata', 'fhv_tripdata']

    for month in months:
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"    
        #url = f"{semi_url}/{file_name}"
        #print(url)
        #clean_parquet_data(url, file_name, service)
         # download it using requests via a pandas df
        
        request_url = f"{init_url}{service}/{file_name}"
        print (request_url)
        urllib.request.urlretrieve(request_url, f"data/{service}/{file_name}")
        #r = requests.get(request_url)
        #open(f'data/{file_name}', 'wb').write(r.content)
        print(f"Local: data/{file_name}")

        # read it back into a parquet file
        df = enforce_dtype(file_name, service)
        file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(f"data/{service}/{file_name}", engine='pyarrow', coerce_timestamps="ms")
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        #upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")
        write_to_bucket(file_name, bucket_name, service, year)

if __name__ == "__main__":
    service = 'fhv'
    year = '2019'
    main(service, year)