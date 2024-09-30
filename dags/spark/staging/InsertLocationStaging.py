
# Import SparkSession
from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.functions import raise_error
from pyspark.sql.types import StructType,StructField
import logging
import os

import quinn
from pyspark.sql.types import *


run_date = os.environ['run_date']
#BatchId = os.environ['BatchId']
logging.warning('run date is {}'.format(run_date) )

spark = SparkSession.builder \
      .master("spark://f5202a576c92:7077") \
      .appName("InsertFromPostgresToStaging")\
      .getOrCreate()

def  get_rawdata(table_name):
  return spark.read.format("jdbc").option("url","jdbc:postgresql://172.21.0.7:5432/airflow")\
    .option("driver","org.postgresql.Driver")\
    .option("query",'select * from "Event_Ticket"."{}" where "CreateDate" = \'{}\' or "UpdateDate" = \'{}\''.format(table_name,run_date,run_date))\
    .option("user","airflow")\
    .option("password","airflow").load()

def get_count(table_name):
  return spark.read.format("jdbc").option("url","jdbc:postgresql://172.21.0.7:5432/airflow")\
    .option("driver","org.postgresql.Driver")\
    .option("query",'select count(*) cnt from "Event_Ticket"."{}" where "CreateDate" = \'{}\' or "UpdateDate" = \'{}\''.format(table_name,run_date,run_date))\
    .option("user","airflow")\
    .option("password","airflow").load()

def validate_count(table_name):
  df = get_rawdata(table_name)
  df_src_cnt = get_count(table_name)
  stg = df.count()
  src = df_src_cnt.collect()[0]['cnt']
  if stg == src:
    return df
  else:
    raise_error('Table {} records count is not matching source'.format(table_name))

def validate_fields_type(source_df):
  tb_schema = StructType(
      List(
        StructField("LocId",IntegerType, False),
        StructField("LocName",StringType, False),
        StructField("StAddress",StringType, True),
        StructField("City",StringType, False),
        StructField("Province",StringType, False),
        StructField("Region",DoubleType, False),
        StructField("CreateDate",DateType, False),
        StructField("UpdateDate",DateType, True),
        StructField("Country",StringType, False),
        StructField("PstCd",StringType, True)
      )
  )
  quinn.validate_schema(source_df, tb_schema)

#check count of records
loc_df = validate_count('Location')

#check existence of columns
quinn.validate_presence_of_columns(loc_df, ["LocId","LocName","StAddress", "City","Province", "Region", "Country", "PstCd", "CreateDate", "UpdateDate"])
#Check Columns Types
validate_fields_type(loc_df)

#valid df
#df = loc_df.withColumn('BatchId',BatchId)
df = loc_df
#insert df to hive staging db
df.write.mode("append").saveAsTable("staging.evnt_pltfrm_location")

