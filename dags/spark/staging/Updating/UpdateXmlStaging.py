
# Import SparkSession
from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.functions import raise_error
from pyspark.sql.types import StructType,StructField
import logging
import os
from datetime import datetime

import quinn
from functools import reduce
from pyspark.sql.types import *


run_date = os.environ['run_date']
BatchId = os.environ['BatchId']
logging.warning('run date is {}'.format(run_date) )

spark = SparkSession.builder \
      .master("spark://f5202a576c92:7077") \
      .appName("InsertFromPostgresToStaging")\
      .getOrCreate()

def get_count(table_name,column_name,column_value):
  return spark.sql("select count(*) cnt from staging.{} where {} = {}".format(table_name,column_name,column_value))

#.schema(schema)
def  get_rawdata(filepath,schema):
  return spark.read.format("com.databricks.spark.xml")\
      .option("rowTag", "xml")\
      .xml(filepath)

def insert_error(df_err):
  df_err.write.format("jdbc")\
    .option("url", "jdbc:postgresql://172.21.0.7:5432/airflow") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "\"Event_Error\".xml_errors") \
    .option("user", "airflow").option("password", "airflow").save()

def validate_count(table_name):
  df = get_rawdata(table_name)
  stg = df.count()
  if stg > 0:
    return df
  else:
    raise_error('Table {} records count is not matching source'.format(table_name))

def validate_fields_type(source_df,schema):
  quinn.validate_schema(source_df, schema)

#check count of records
#sqlContext.sql("select Sale.Tax['@TaxRate'] as TaxRate from temptable").show();
tb_schema = StructType(
      List(
        StructField("transaction",StructType(
            List(
                StructField("date", StringType, False),
                StructField("reseller-id", StringType, False)
            )
        )),
        StructField("tranactionId",IntegerType, False),
        StructField("eventName",StringType, False),
        StructField("numberOfPurchasedTickets",IntegerType, False),
        StructField("totalAmount",DoubleType, True),
        StructField("salesChannel",StringType, False),
        StructField("Customer",StructType(
            List(
                StructField("first_name",StringType, False),
                StructField("last_name",StringType, True)
                )
        )),
        StructField("officeLocation",IntegerType, True),
        StructField("dateCreated",DateType, False)
      )
  )
trans_df = validate_count('XMLFile.xml',tb_schema)

#check existence of columns
quinn.validate_presence_of_columns(trans_df, ["TransId","EventName", "Ticket_no", "Amount","channel", "first_name","last_name", "officeLocId", "CreateDate"])
#Check Columns Types
validate_fields_type(trans_df)
trans_df = trans_df.withColumnRenamed("reseller-id","reseller_id")
trans_df.createOrReplaceTempView("trans_tb")

#get needed lookup tables from staging
ev_tb = spark.sql("select * from staging.evnt_pltfrm_events")
ev_tb.createOrReplaceTempView("ev_tb")
loc_tb = spark.sql("select * from staging.evnt_pltfrm_location")
loc_tb.createOrReplaceTempView("loc_tb")
res_tb = spark.sql("select * from staging.evnt_pltfrm_reseller")
res_tb.createOrReplaceTempView("res_tb")

#get prev errors
prev_err = spark.read.format("jdbc").option("url","jdbc:postgresql://172.21.0.7:5432/airflow")\
    .option("driver","org.postgresql.Driver")\
    .option("query",'select transID from "\"Event_Error\".xml_errors"')\
    .option("user","airflow")\
    .option("password","airflow").load()
prev_err.createOrReplaceTempView("prev_err")


#validate  existing event/ existing location
#to validate : / trans date between event_creatdt,rundt-1, ticket_no*cost=Amount, amount >0
df_Tovalid = spark.sql("""
                  select 
                    trans_tb.* ,
                    ev_tb.EventName as event_to_validated,
                    loc_tb.LocName as loc_to_validated,
                    res_tb.ResellerId as reseller_to_validated,
                    case
                      when trans_tb.Amount = trans_tb.Purchased_ticket * ev_tb.TicketCost then 1
                      else 0
                    end as valid_ticket,
                    case
                      when trans_tb.createDate BETWEEN ev_tb.CreateDate AND {} then 1
                      else 0
                    end as valid_date
                  from trans_tb 
                  inner join prev_err on prev_err.transID = trans_tb.transID
                  left join ev_tb on ev_tb.EventName = trans_tb.EventName
                  left join loc_tb on loc_tb.LocName = trans_tb.officeLocation
                  left join res_tb on res_tb.ResellerId = trans_tb.reseller_id
                    """.format(run_date))
df_Tovalid.createOrReplaceTempView("df_Tovalid")


#valid df
df_valid = spark.sql("""
            select 
              transID, eventName, Purchased_ticket, Amount, channel, 
              custFirstName, custLastName, officeLoc, createDate
            from df_Tovalid
            where event_to_validated is not null
            and loc_to_validated is not null
            and reseller_to_validated is not null
            and valid_ticket = 1
            and valid_date = 1
          """)
df = trans_df.withColumn('BatchId',BatchId)

#insert df to hive staging db
df.write.mode("append").saveAsTable("staging.xml_transaction")

for row in df_Tovalid.select("transID").collect():
    trans = row['transID']
    spark.write.format("jdbc")\
    .option("url", "jdbc:postgresql://172.21.0.7:5432/airflow") \
    .option("driver", "org.postgresql.Driver") \
    .option("query","update \"Event_Error\".xml_errors set reprocessed='Y' where transID={}".format(trans))\
    .option("user", "airflow").option("password", "airflow").save()

#insert invalid event into error table
df_invalid_event = spark.sql("""
            select 
              transID,
              'invalid_event' as comment,
              'N' as reprocessed,
              {} as createDt,
              null as updateDt
            from df_Tovalid
            where event_to_validated is null
          """.format(run_date))
insert_error(df_invalid_event)

#insert invalid location into error table
df_invalid_loc = spark.sql("""
            select 
              transID,
              'invalid_location' as comment,
              'N' as reprocessed,
              {} as createDt,
              null as updateDt
            from df_Tovalid
            where loc_to_validated is null
          """.format(run_date))
insert_error(df_invalid_loc)

#insert invalid ticket cost/amount into error table
df_invalid_ticket = spark.sql("""
            select 
              transID,
              'invalid_ticket_amount' as comment,
              'N' as reprocessed,
              {} as createDt,
              null as updateDt
            from df_Tovalid
            where valid_ticket = 0
          """.format(run_date))
insert_error(df_invalid_ticket)

#insert invalid date into error table
df_invalid_dt = spark.sql("""
            select 
              transID,
              'invalid_date' as comment,
              'N' as reprocessed,
              {} as createDt,
              null as updateDt
            from df_Tovalid
            where valid_date = 0
          """.format(run_date))
insert_error(df_invalid_dt)

#insert invalid reseller into error table
df_invalid_dt = spark.sql("""
            select 
              transID,
              'invalid_date' as comment,
              'N' as reprocessed,
              {} as createDt,
              null as updateDt
            from df_Tovalid
            where reseller_to_validated is null
          """.format(run_date))
insert_error(df_invalid_dt)
