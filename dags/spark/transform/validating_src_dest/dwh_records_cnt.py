# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import raise_error
import os

run_date = os.environ['run_date']
#BatchId = os.environ['BatchId']
src_table = os.environ['src_tbl']
dest_table = os.environ['dest_tbl']
dup_col = os.environ['dup_col']


spark = SparkSession.builder \
      .master("spark://f5202a576c92:7077") \
      .appName("CheckCountStagingDwh").enableHiveSupport()\
      .getOrCreate()

def get_count_dwh():
    return spark.read.format("jdbc").option("url","jdbc:postgresql://172.21.0.7:5432/airflow")\
    .option("driver","org.postgresql.Driver")\
    .option("query",'select count(*) cnt from "Event_Dwh"."{}" where "CreateDate" = \'{}\' or "UpdateDate" = \'{}\''.format(dest_table,run_date,run_date))\
    .option("user","airflow")\
    .option("password","airflow").load()

def delete_dwh():
    return spark.write.format("jdbc").option("url","jdbc:postgresql://172.21.0.7:5432/airflow")\
    .option("driver","org.postgresql.Driver")\
    .option("query",'delete from "Event_Dwh"."{}" where "CreateDate" = \'{}\' or "UpdateDate" = \'{}\''.format(dest_table,run_date,run_date))\
    .option("user","airflow")\
    .option("password","airflow").save()

def get_count_src(): 
    return spark.sql("select * cnt from staging.{} where CreateDate = '{}' or UpdateDate = '{}'".format(src_table,run_date,run_date))

src = get_count_src().dropDuplicates([dup_col]).count()
dest = get_count_dwh().collect()[0]['cnt']

if src != dest:
    delete_dwh()
    raise_error("source table {} is not matching destination table {}".format(src_table,dest_table))