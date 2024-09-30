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
      .appName("InsertToDwh").enableHiveSupport()\
      .getOrCreate()

def getRawDate(table_name):
      return spark.sql("select * from staging.{} where CreateDate '{}' or UpdateDate = '{}'").format(table_name,run_date,run_date)

def get_count(table_name):
      return spark.sql("select count(*) cnt from staging.{} where CreateDate '{}' or UpdateDate = '{}'").format(table_name,run_date,run_date)

def validate_count(table_name):
      df = getRawDate(table_name)
      df_src_cnt = get_count(table_name)
      stg = df.count()
      src = df_src_cnt.collect()[0]['cnt']
      if stg == src:
            return df.dropDuplicates(['CustId'])
      else:
            raise_error('Table {} records count is not matching source'.format(table_name))

cust_df = validate_count('evnt_pltfrm_customer')
cust_tb = cust_df.createOrReplaceTempView("temp_cust")

calendar = spark.read.format("jdbc").option("url","jdbc:postgresql://172.21.0.7:5432/airflow")\
    .option("driver","org.postgresql.Driver")\
    .option("query",'select date_dim_id,date_actual from "Event_Ticket"."d_date"')\
    .option("user","airflow")\
    .option("password","airflow").load()
cldr_tb = calendar.createOrReplaceTempView("temp_cldr")

cust_tf = spark.sql("""select 
                  v.CustId As Src_cust_id,
                  v.FirstName As Fname,
                  v.LastName As Lname,
                  v.Pmail,
                  v.PhoneNo,
                  c.date_dim_id As createDt_id,
                  u.date_dim_id As UpdateDt_id
            FROM temp_cust v 
            inner join temp_cldr c on v.CreateDate = c.date_actual
            left join temp_cldr u on v.UpdateDate = u.date_actual
            where v.UpdateDate is null
            """)

cust_tf.write.format("jdbc").option("url","jdbc:postgresql://172.21.0.7:5432/airflow")\
    .option("driver","org.postgresql.Driver").option("dbtable",'"Event_Dwh"."Customer"')\
    .option("user","airflow")\
    .option("password","airflow").save()

cust_tf_update = spark.sql("""select 
                  v.CustId As Src_cust_id,
                  v.FirstName As Fname,
                  v.LastName As Lname,
                  v.Pmail,
                  v.PhoneNo,
                  c.date_dim_id As createDt_id,
                  u.date_dim_id As UpdateDt_id
            FROM temp_cust v 
            inner join temp_cldr c on v.CreateDate = c.date_actual
            inner join temp_cldr u on v.UpdateDate = u.date_actual
            where v.UpdateDate is not null
            """)
for row in cust_tf_update.collect():
      spark.write.format("jdbc").option("url","jdbc:postgresql://172.21.0.7:5432/airflow")\
      .option("driver","org.postgresql.Driver")\
      .option("query","""update "Event_Dwh"."Customer" 
                              set Fname='{}' ,Lname = '{}',
                                    Pmail='{}',PhoneNo='{}',UpdateDt_id='{}'
                              where Src_cust_id = {}
                  """.format(row['Fname'], row['Lname'], row['Pmail'], row['PhoneNo'], row['UpdateDt_id'], row['Src_cust_id']))\
      .option("user","airflow")\
      .option("password","airflow").save()