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

def get_count(table_name):
      return spark.sql("select count(*) cnt from staging.{} where CreateDate '{}' or UpdateDate = '{}'").format(table_name,run_date,run_date)

def validate_count(table_name):
      df = getRawDate(table_name)
      df_src_cnt = get_count(table_name)
      stg = df.count()
      src = df_src_cnt.collect()[0]['cnt']
      if stg == src:
            return df.dropDuplicates(['ResellerId'])
      else:
            raise_error('Table {} records count is not matching source'.format(table_name))

res_df = validate_count('evnt_pltfrm_reseller')
res_df.createOrReplaceTempView("temp_res")

calendar = spark.read.format("jdbc").option("url","jdbc:postgresql://172.21.0.7:5432/airflow")\
    .option("driver","org.postgresql.Driver")\
    .option("query",'select date_dim_id,date_actual from "Event_Ticket"."d_date"')\
    .option("user","airflow")\
    .option("password","airflow").load()
cldr_tb = calendar.createOrReplaceTempView("temp_cldr")

vnu_tf = spark.sql("""select 
                  v.ResellerId As src_res_id,
                  v.FirstName As fName,
                  v.LastName As lName,
                  c.date_dim_id As createDt_id,
                  u.date_dim_id As UpdateDt_id
            FROM temp_res v 
            inner join temp_cldr c on v.CreateDate = c.date_actual
            left join temp_cldr u on v.UpdateDate = u.date_actual
            where v.UpdateDate is null
            """)

vnu_tf.write.format("jdbc").option("url","jdbc:postgresql://172.21.0.7:5432/airflow")\
    .option("driver","org.postgresql.Driver").option("dbtable",'"Event_Dwh"."Reseller"')\
    .option("user","airflow")\
    .option("password","airflow").save()

vnu_tf_update = spark.sql("""select 
                  v.ResellerId As src_res_id,
                  v.FirstName As fName,
                  v.LastName As lName,
                  c.date_dim_id As createDt_id,
                  u.date_dim_id As UpdateDt_id
            FROM temp_res v 
            inner join temp_cldr c on v.CreateDate = c.date_actual
            left join temp_cldr u on v.UpdateDate = u.date_actual
            where v.UpdateDate is not null
            """)

for row in vnu_tf_update.collect():
      spark.write.format("jdbc").option("url","jdbc:postgresql://172.21.0.7:5432/airflow")\
      .option("driver","org.postgresql.Driver")\
      .option("query","""update "Event_Dwh"."Reseller" 
                              set fName='{}' ,lName = '{}',
                                    UpdateDt_id='{}'
                              where src_res_id = {}
                  """.format(row['fName'], row['lName'], row['UpdateDt_id'], row['src_res_id']))\
      .option("user","airflow")\
      .option("password","airflow").save()