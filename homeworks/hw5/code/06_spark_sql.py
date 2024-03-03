#!/usr/bin/env python
# coding: utf-8

# In[1]:
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("spark://ip-172-31-22-115.ec2.internal:7077") \
    .appName('test') \
    .getOrCreate()

# Read some data
# In[4]:

df_green = spark.read.parquet('data/pq/fhv/*/*')

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

df_result.coalesce(1).write.parquet('data/report/revenue/', mode='overwrite')

# In[ ]:




