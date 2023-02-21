import pandas as pd
from pathlib import Path
from datetime import timedelta
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

# from prefect.tasks import task_input_hash
# from random import randint

# For some reason, the commented @task below is responsible for an exception when deploying the code using
# the docker block as the infrastructure. It has something to do with cache_key_fn and cache_expiration.
# I discovered this solution by reading a thread in the course's Slack.
# See:
# https://datatalks-club.slack.com/archives/C01FABYF2RG/p1674823816614039
# https://github.com/PrefectHQ/prefect/issues/6086
# @task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
@task(retries=1)
def fetch(url: str) -> pd.DataFrame:
    
    df = pd.read_csv(url, compression='gzip',  encoding='latin1')
    
    
    df = df.convert_dtypes()
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])
    
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out as parquet file"""
    data_dir = f'data/fhv'
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    path = Path(f'{data_dir}/{dataset_file}.parquet')
    print(f'Path: {path}')
    df.to_parquet(path, compression='gzip')
    return path

@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    to_path = Path(path).as_posix()
    print(f'To Path: {to_path}')
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=to_path)

@flow()
def etl_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f'fhv_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz'
    df = fetch(dataset_url)
    path = write_local(df, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(months: list[int] = [11], year: int = 2020) -> None:
    for month in months:
        etl_web_to_gcs(year, month)

if __name__ == '__main__':

    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    year = 2019
    etl_parent_flow(months, year)