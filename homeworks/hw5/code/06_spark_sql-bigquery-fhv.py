#!/usr/bin/env python
# coding: utf-8

# In[1]:
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('test').getOrCreate()
# Connect to a temporary bucket
bucket='dataproc-temp-us-east1-1084011134216-ofyvqcfq'
spark.conf.set('temporaryGcsBucket', bucket)

# Read some data
# In[4]:

df_green = spark.read.parquet('gs://mage-zoomcamp-ems/spark/pq/*')

# In[5]:

df_green.show(5)

# In[6]:

df_green.registerTempTable('fhv_data')

# In[7]:

spark.sql("""
SELECT
    dispatching_base_num,
    count(1)
FROM
    fhv_data
GROUP BY 
    dispatching_base_num
""").show()

# In[8]:

df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PUlocationID AS trips_zone,
    date_trunc('day', pickup_datetime) AS trips_day, 
    COUNT(1) as trips
FROM
    fhv_data
GROUP BY
    1, 2
""")

# In[9]:
output="ny_taxi.reports-fhv-201910"
df_result.write.format('bigquery') \
    .option('table', output) \
    .save()

# In[ ]:




