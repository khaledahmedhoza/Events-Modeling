
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
BatchId = os.environ['BatchId']
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
        StructField("TransId",IntegerType, False),
        StructField("CustId",IntegerType, False),
        StructField("EventId",IntegerType, False),
        StructField("Channel",StringType, False),
        StructField("CreateDate",TimestampType, False),
        StructField("UpdateDate",TimestampType, True),
        StructField("TicketsCnt",IntegerType, False),
        StructField("Amount",DoubleType, False),
        StructField("delFlag",StringType, True),
        StructField("Reseller_id",IntegerType, True)
      )
  )
  quinn.validate_schema(source_df, tb_schema)

#check count of records
trans_df = validate_count('Transaction')

#check existence of columns
quinn.validate_presence_of_columns(trans_df, ["TransId","Reseller_id", "delFlag", "CustId","EventId", "Channel", "TicketsCnt", "Amount", "CreateDate", "UpdateDate"])
#Check Columns Types
validate_fields_type(trans_df)

#valid df
df = trans_df.withColumn('BatchId',BatchId)

#insert df to hive staging db
df.write.mode("append").saveAsTable("staging.evnt_pltfrm_transaction")

