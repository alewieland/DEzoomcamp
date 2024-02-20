#!/usr/bin/env python
# coding: utf-8
# author: alewieland <alexander.wieland.aw@gmail.com>
# date : 2024-02-19
"""
    Program that download the taxi data parquet files
    needed for module, and load them into cloud storage
    --> define the GOOGLE_APPLICATION_CREDENTIALS env variable
"""

import os
import urllib.request
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


def main(service, year):
    """Main for the program"""
    # Name of the bucket 
    bucket_name = 'de-zoomcamp-bucket-drux'
    #folder name
    semi_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    
    months = ['01', '02', '03', '04', '05', '06', 
              '07', '08', '09', '10', '11', '12']
    #years = ['2019', '2020']
    #folder_names = ['green_tripdata', 'yellow_tripdata', 'fhv_tripdata']

    for month in months:
        file_name = f'{service}_{year}-{month}.parquet'    
        url = f"{semi_url}/{file_name}"
        #print(url)
        urllib.request.urlretrieve(url, f"data/{service}/{file_name}")
        write_to_bucket(file_name, bucket_name, service, year)

if __name__ == "__main__":
    #service = 'fhv_tripdata'
    #year = '2019'
    years = ['2019', '2020']
    services = ['green_tripdata', 'yellow_tripdata']
    for service in services:
        for year in years:
            main(service, year)