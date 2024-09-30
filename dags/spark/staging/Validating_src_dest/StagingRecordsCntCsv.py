# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import raise_error
import os

run_date = os.environ['run_date']
#BatchId = os.environ['BatchId']
src_table = os.environ['src_tbl']
dup_col = os.environ['dup_col']

spark = SparkSession.builder \
      .master("spark://f5202a576c92:7077") \
      .appName("InsertFromPostgresToStaging").enableHiveSupport()\
      .getOrCreate()

def get_count_src():
    return spark.read.csv(src_table).count()

def get_count_staging():
    return spark.sql("select * cnt from staging.spreadsheet_transaction where CreateDate = '{}' or UpdateDate = '{}'".format(run_date,run_date))

src = get_count_src()
dest = get_count_staging().dropDuplicates([dup_col]).count()

if src != dest:
    raise_error("source table {} is not matching destination table staging.spreadsheet_transaction".format(src_table))