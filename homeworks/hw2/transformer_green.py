if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd


@transformer
def transform(data, *args, **kwargs):
    # remove rows with trip_distance equal zero or passenger_count equal zero
    data=data.loc[(data['trip_distance'] != 0) & (data['passenger_count'] != 0)]

    old_names = list(data.columns)
    
    #rename cols from CamelCase to SnakeCase
    data.columns = (data.columns
                    .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                    .str.lower()
                )
    
    new_names = list(data.columns)

    print('column names changed: ', set(old_names) - (set(old_names).intersection(new_names)))
    
    print('unique values in vendor_id column: ', list(data['vendor_id'].unique()))
    
    #derive date from existing column and change format to datetime
    data['lpep_pickup_date']= data['lpep_pickup_datetime'].dt.date
    data['lpep_pickup_date'] = pd.to_datetime(data['lpep_pickup_date'])

    return data


@test
def test_output(output, *args) -> None:

    assert (output['vendor_id'].isin([1, 2])).all(), 'There are unexpected values in vendor_id column'
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passenger count'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with trip_distance equals to zero'
    
