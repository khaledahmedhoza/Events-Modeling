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
            return df.dropDuplicates(['EventId'])
      else:
            raise_error('Table {} records count is not matching source'.format(table_name))

res_df = validate_count('evnt_pltfrm_events')
res_df.createOrReplaceTempView("temp_res")

calendar = getDwhData("d_date")
calendar.createOrReplaceTempView("temp_cldr")

location = getDwhData("Location")
location.createOrReplaceTempView("temp_loc")

venue = getDwhData("Venue")
venue.createOrReplaceTempView("temp_vnu")

vnu_tf = spark.sql("""select 
                  v.EventId As src_event_id,
                  c.date_dim_id As createDt_id,
                  u.date_dim_id As UpdateDt_id,
                  s.date_dim_id As startDt_id,
                  e.date_dim_id As endDt_id,
                  v.EventName As event_name,
                  v.EventType As event_type,
                  v.TicketCost As ticket_cost,
                  l.row_wid As LocId,
                  vn.row_wid As Venue_id
            FROM temp_res v 
            inner join temp_cldr c on v.CreateDate = c.date_actual
            left join temp_cldr u on v.UpdateDate = u.date_actual
            left join temp_cldr s on v.StartDate = s.date_actual
            left join temp_cldr e on v.EndDate = e.date_actual
            left join temp_loc l on l.src_loc_id = v.LocId
            left join temp_vnu vn on vn.src_Venue_id = v.VenueId
            where v.UpdateDate is null
            """)

vnu_tf.write.format("jdbc").option("url","jdbc:postgresql://172.21.0.7:5432/airflow")\
    .option("driver","org.postgresql.Driver").option("dbtable",'"Event_Dwh"."Event"')\
    .option("user","airflow")\
    .option("password","airflow").save()

vnu_tf_update = spark.sql("""select 
                  v.EventId As src_event_id,
                  c.date_dim_id As createDt_id,
                  u.date_dim_id As UpdateDt_id,
                  s.date_dim_id As startDt_id,
                  e.date_dim_id As endDt_id,
                  v.EventName As event_name,
                  v.EventType As event_type,
                  v.TicketCost As ticket_cost,
                  l.row_wid As LocId,
                  vn.row_wid As Venue_id
            FROM temp_res v 
            inner join temp_cldr c on v.CreateDate = c.date_actual
            inner join temp_cldr u on v.UpdateDate = u.date_actual
            left join temp_cldr s on v.StartDate = s.date_actual
            left join temp_cldr e on v.EndDate = e.date_actual
            left join temp_loc l on l.src_loc_id = v.LocId
            left join temp_vnu vn on vn.src_Venue_id = v.VenueId
            where v.UpdateDate is not null
            """)

for row in vnu_tf_update.collect():
      spark.write.format("jdbc").option("url","jdbc:postgresql://172.21.0.7:5432/airflow")\
      .option("driver","org.postgresql.Driver")\
      .option("query","""update "Event_Dwh"."Event" 
                              set UpdateDt_id='{}' ,startDt_id = '{}',
                                    endDt_id='{}',EventName='{}'
                              where src_event_id = {}
                  """.format(row['UpdateDt_id'], row['startDt_id'], row['endDt_id'], row['EventName'], row['src_event_id']))\
      .option("user","airflow")\
      .option("password","airflow").save()