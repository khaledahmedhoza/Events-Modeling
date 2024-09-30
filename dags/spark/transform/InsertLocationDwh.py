# Import SparkSession
from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.functions import raise_error
from pyspark.sql.types import StructType,StructField
import logging
import os
#os.system('pip3 install quinn')
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

def getDwhData(table_name):
      return spark.read.format("jdbc").option("url","jdbc:postgresql://172.21.0.7:5432/airflow")\
            .option("driver","org.postgresql.Driver")\
            .option("query",'select * from "Event_Dwh"."{}"'.format(table_name))\
            .option("user","airflow")\
            .option("password","airflow").load()

def get_count(table_name):
      return spark.sql("select count(*) cnt from staging.{} where CreateDate '{}' or UpdateDate = '{}'").format(table_name,run_date,run_date)

def validate_count(table_name):
      df = getRawDate(table_name)
      df_src_cnt = get_count(table_name)
      stg = df.count()
      src = df_src_cnt.collect()[0]['cnt']
      if stg == src:
            return df.dropDuplicates(['LocId'])
      else:
            raise_error('Table {} records count is not matching source'.format(table_name))

loc_df = validate_count('evnt_pltfrm_location')
loc_df.createOrReplaceTempView("temp_loc")

calendar = getDwhData("d_date")
calendar.createOrReplaceTempView("temp_cldr")

Loc_tf = spark.sql("""select 
                  v.LocId As src_loc_id,
                  v.LocName As Loc_name,
                  v.StAddress ,
                  v.City,
                  v.Province,
                  v.Region,
                  v.Country,
                  v.PstCd,
                  c.date_dim_id As createDt_id,
                  u.date_dim_id As UpdateDt_id
            FROM temp_loc v 
            inner join temp_cldr c on v.CreateDate = c.date_actual
            left join temp_cldr u on v.UpdateDate = u.date_actual
            where v.UpdateDate is null
            """)

Loc_tf.write.format("jdbc").option("url","jdbc:postgresql://172.21.0.7:5432/airflow")\
    .option("driver","org.postgresql.Driver").option("dbtable",'"Event_Dwh"."Location"')\
    .option("user","airflow")\
    .option("password","airflow").save()

Loc_tf_update = spark.sql("""select 
                  v.LocId As src_loc_id,
                  v.LocName As Loc_name,
                  v.StAddress ,
                  v.City,
                  v.Province,
                  v.Region,
                  v.Country,
                  v.PstCd,
                  c.date_dim_id As createDt_id,
                  u.date_dim_id As UpdateDt_id
            FROM temp_loc v 
            inner join temp_cldr c on v.CreateDate = c.date_actual
            left join temp_cldr u on v.UpdateDate = u.date_actual
            where v.UpdateDate is not null
            """)

for row in Loc_tf_update.collect():
      spark.write.format("jdbc").option("url","jdbc:postgresql://172.21.0.7:5432/airflow")\
      .option("driver","org.postgresql.Driver")\
      .option("query","""update "Event_Dwh"."Location" 
                              set UpdateDt_id='{}' ,Loc_name = '{}'
                              where LocId = {}
                  """.format(row['UpdateDt_id'], row['Loc_name'], row['LocId']))\
      .option("user","airflow")\
      .option("password","airflow").save()