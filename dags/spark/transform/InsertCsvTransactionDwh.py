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
            return df.dropDuplicates(['TransId'])
      else:
            raise_error('Table {} records count is not matching source'.format(table_name))

pltfrm_trans = validate_count('spreadsheet_transaction')
pltfrm_trans.createOrReplaceTempView("temp_pltfrm_trans")


calendar = getDwhData("d_date")
calendar.createOrReplaceTempView("temp_cldr")

event = getDwhData("Event")
event.createOrReplaceTempView("temp_evnt")

venue = getDwhData("Venue")
venue.createOrReplaceTempView("temp_vnu")

cust = getDwhData("Customer")
cust.createOrReplaceTempView("temp_cust")

location = getDwhData("Location")
location.createOrReplaceTempView("temp_loc")

comm = spark.sql("select * from staging.evnt_pltfrm_commission")
comm.createOrReplaceTempView("temp_comm")

reseller = getDwhData("Reseller")
reseller.createOrReplaceTempView("temp_res")

Loc_tf = spark.sql("""select 
                  v.TransId As src_trans_id,
                  c.date_dim_id As createDt_id,
                  '' As UpdateDt_id,
                  e.row_wid As event_id,
                  vnu.row_wid As venue_id ,
                  cust.row_wid As customer_id,
                  res.row_wid As reseller_id,
                  'Csv_3rdparthy_platfrom' As sold_by,
                  v.Ticket_no As tickets_sold,
                  v.Amount As total_amount,
                  'N' As del_flg,
                  v.BatchId,
                  loc.row_wid As OfficeLocId,
                  v.Channel ,
                  com.Commission 
            FROM temp_pltfrm_trans v 
            inner join temp_cldr c on v.CreateDate = c.date_actual
            inner join temp_evnt e on v.EventName = e.event_name
            inner join temp_cust cust on v.first_name = cust.FirstName AND v.last_name = cust.LastName 
            inner join temp_vnu vnu on e.VenueId = vnu.src_venue_id
            inner join temp_loc loc on v.officeLocId = loc.src_loc_id
            inner join temp_comm com on e.EventId = com.EventId and com.ResId = v.reseller_id
            inner join temp_res res on res.src_res_id = v.reseller_id
            where v.UpdateDate is null
            """)

Loc_tf.write.format("jdbc").option("url","jdbc:postgresql://172.21.0.7:5432/airflow")\
    .option("driver","org.postgresql.Driver").option("dbtable",'"Event_Dwh"."Transaction"')\
    .option("user","airflow")\
    .option("password","airflow").save()

Loc_tf_update = spark.sql("""select 
                  v.TransId As src_trans_id,
                  c.date_dim_id As createDt_id,
                  '' As UpdateDt_id,
                  e.row_wid As event_id,
                  vnu.row_wid As venue_id ,
                  cust.row_wid As customer_id,
                  res.row_wid As reseller_id,
                  'Csv_3rdparthy_platfrom' As sold_by,
                  v.Ticket_no As tickets_sold,
                  v.Amount As total_amount,
                  'N' As del_flg,
                  v.BatchId,
                  loc.row_wid As OfficeLocId,
                  v.Channel ,
                  com.Commission 
            FROM temp_pltfrm_trans v 
            inner join temp_cldr c on v.CreateDate = c.date_actual
            inner join temp_evnt e on v.EventName = e.event_name
            inner join temp_cust cust on v.first_name = cust.FirstName AND v.last_name = cust.LastName 
            inner join temp_vnu vnu on e.VenueId = vnu.src_venue_id
            inner join temp_loc loc on v.officeLocId = loc.src_loc_id
            inner join temp_comm com on e.EventId = com.EventId and com.ResId = v.reseller_id
            inner join temp_res res on res.src_res_id = v.reseller_id
            where v.UpdateDate is not null
            """)

for row in Loc_tf_update.collect():
      spark.write.format("jdbc").option("url","jdbc:postgresql://172.21.0.7:5432/airflow")\
      .option("driver","org.postgresql.Driver")\
      .option("query","""update "Event_Dwh"."Transaction" 
                              set UpdateDt_id='{}' ,del_flg = '{}'
                              where src_trans_id = {} and sold_by='Csv_3rdparthy_platfrom'
                  """.format(row['UpdateDt_id'], row['del_flg'],row['src_trans_id']))\
      .option("user","airflow")\
      .option("password","airflow").save()