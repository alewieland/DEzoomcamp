
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
from google.cloud import storage

def write_to_bucket(filename, bucket_name, service, year):
    """Write green data parquet file into the given bucket"""
    # Instasiate a client
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    
    destination_blob = bucket.blob(f'{service}/{year}/{filename}')

    destination_blob.upload_from_filename(f"data/{service}/{filename}")
    print(f"file data/{service}/{filename} uploaded to {destination_blob}")


#def extract_data(url, month):
#    """Doanload the data from the url localy"""
#    file_name = f"green_data/green_tripdata_2022-{month}.parquet"
#    os.system(f'wget {url} -O {file_name}')
#    return file_name
def clean_parquet_data(url, filename, service):
    """
    Function for cleaning the parquet file and make sure
    all the files share the same data types
    """
    green_taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RatecodeID': pd.Int64Dtype(),
                    'store_and_fwd_flag': str,
                    'PULocationID': pd.Int64Dtype(),
                    'DOLocationID': pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra': float,
                    'mta_tax': float,
                    'tip_amount': float,
                    'tolls_amount': float,
                    'ehail_fee': float,
                    'improvement_surcharge': float,
                    'total_amount': float,
                    'congestion_surcharge': float
                }
    #green_parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

    yellow_taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RatecodeID': pd.Int64Dtype(),
                    'store_and_fwd_flag': str,
                    'PULocationID': pd.Int64Dtype(),
                    'DOLocationID': pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra': float,
                    'mta_tax': float,
                    'tip_amount': float,
                    'tolls_amount': float,
                    'improvement_surcharge': float,
                    'total_amount': float,
                    'congestion_surcharge': float,
                    'airport_fee': float
                }
    #yellow_parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    
    fhv_dtypes = {
                    'dispatching_base_num': str,
                    'PUlocationID': pd.Int64Dtype(),
                    'DOlocationID': pd.Int64Dtype(),
                    'SR_Flag': pd.Int64Dtype()
                }
    df = pd.read_parquet(url)
    match service:
        case 'green_tripdata':
            df = pd.read_parquet(url)
            #print(df.dtypes)
            df = df.astype(green_taxi_dtypes)
        case 'yellow_tripdata':
            df = pd.read_parquet(url)
            df = df.astype(yellow_taxi_dtypes)
        case 'fhv_tripdata':
            df = pd.read_parquet(url)
            df = df.astype(fhv_dtypes)

    df_table = pa.Table.from_pandas(df)
    print(f'saving parquet file to data/{service}/{filename}')
    pq.write_table(df_table,f'data/{service}/{filename}')
            
    return 0


def main(service, year):
    """Main for the program"""
    # Name of the bucket 
    bucket_name = 'alewieland-de-zoomcamp'
    #folder name
    semi_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    
    months = ['01', '02', '03', '04', '05', '06', 
              '07', '08', '09', '10', '11', '12']
    #years = ['2019', '2020']
    #folder_names = ['green_tripdata', 'yellow_tripdata', 'fhv_tripdata']

    for month in months:
        file_name = f'{service}_{year}-{month}.parquet'    
        #url = f"{semi_url}/{file_name}"
        #print(url)
        #urllib.request.urlretrieve(url, f"data/{service}/{file_name}")
        #clean_parquet_data(url, file_name, service)
        write_to_bucket(file_name, bucket_name, service, year)

if __name__ == "__main__":
    service = 'fhv_tripdata'
    year = '2019'
    main(service, year)

    years = ['2019', '2020']
    services = ['green_tripdata', 'yellow_tripdata']
    for service in services:
        for year in years:
            main(service, year)