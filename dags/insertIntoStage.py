from airflow import DAG

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import datetime
import logging



dag = DAG(
    'B2B_Event_Insert_Into_Staging',
    start_date=datetime.datetime(2022,12,1,0,0,0,0),
    schedule_interval="@daily"
)

trigger_dependent_dag = TriggerDagRunOperator(
        task_id="trigger_dependent_dag",
        trigger_dag_id="B2B_Event_Insert_Into_Dwh",
        wait_for_completion=True
    )

Insert_commission = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/staging/InsertCommissionStaging.py' ,
		conn_id= 'spark_local', 
		task_id='Insert_commission', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'' }
		)

Insert_customer = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/staging/InsertCustomerStaging.py' ,
		conn_id= 'spark_local', 
		task_id='Insert_customer', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'' }
		)

Insert_event = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/staging/InsertEventStaging.py' ,
		conn_id= 'spark_local', 
		task_id='Insert_event', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'' }
		)

Insert_location = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/staging/InsertLocationStaging.py' ,
		conn_id= 'spark_local', 
		task_id='Insert_location', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'' }
		)

Insert_Reseller = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/staging/InsertResellerStaging.py' ,
		conn_id= 'spark_local', 
		task_id='Insert_Reseller', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'' }
		)

Insert_transaction = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/staging/InsertTransactionStaging.py' ,
		conn_id= 'spark_local', 
		task_id='Insert_transaction', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'' }
		)

Insert_venue = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/staging/InsertVenueStaging.py' ,
		conn_id= 'spark_local', 
		task_id='Insert_venue', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'' }
		)

Insert_CSV = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/staging/InsertCsvStaging.py' ,
		conn_id= 'spark_local', 
		task_id='Insert_csv', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'' }
		)

Insert_XML = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/staging/InsertXmlStaging.py' ,
		conn_id= 'spark_local', 
		task_id='Insert_xml', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'' }
		)

Cnt_cust = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/staging/Validating_src_dest/StagingRecordsCnt.py' ,
		conn_id= 'spark_local', 
		task_id='count_customer', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'', 'src_tbl':'Customer','dest_tbl':'evnt_pltfrm_customer' , 'dup_col':'CustId'  }
		)

Cnt_evnt = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/staging/Validating_src_dest/StagingRecordsCnt.py' ,
		conn_id= 'spark_local', 
		task_id='count_events', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'', 'src_tbl':'Events','dest_tbl':'evnt_pltfrm_events' , 'dup_col':'EventId' }
		)

Cnt_loc = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/staging/Validating_src_dest/StagingRecordsCnt.py' ,
		conn_id= 'spark_local', 
		task_id='count_location', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'', 'src_tbl':'Location','dest_tbl':'evnt_pltfrm_location' , 'dup_col':'LocId' }
		)

Cnt_res = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/staging/Validating_src_dest/StagingRecordsCnt.py' ,
		conn_id= 'spark_local', 
		task_id='count_reseller', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'', 'src_tbl':'Reseller','dest_tbl':'evnt_pltfrm_reseller' , 'dup_col':'ResellerId'  }
		)

Cnt_trans = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/staging/Validating_src_dest/StagingRecordsCnt.py' ,
		conn_id= 'spark_local', 
		task_id='count_transactions', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'', 'src_tbl':'Transaction','dest_tbl':'evnt_pltfrm_transaction' ,'dup_col':'TransId' }
		)

Cnt_vnu = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/staging/Validating_src_dest/StagingRecordsCnt.py' ,
		conn_id= 'spark_local', 
		task_id='count_venue', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'', 'src_tbl':'Venue','dest_tbl':'evnt_pltfrm_venue' , 'dup_col':'VenueId' }
		)

Cnt_csv = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/staging/Validating_src_dest/StagingRecordsCntCsv.py' ,
		conn_id= 'spark_local', 
		task_id='count_csv_transactions', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'', 'src_tbl':'DailySales_0825022_111222333.csv', 'dup_col':'TransId'  }
		)

Cnt_xml = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/staging/Validating_src_dest/StagingRecordsCntXml.py' ,
		conn_id= 'spark_local', 
		task_id='count_xml_transactions', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'', 'src_tbl':'XMLFile.xml' , 'dup_col':'TransId'}
		)

Insert_commission >> Insert_customer
Insert_customer >> Cnt_cust
Cnt_cust >> Insert_event
Insert_event >> Cnt_evnt
Cnt_evnt >> Insert_location
Insert_location >> Cnt_loc
Cnt_loc >> Insert_Reseller
Insert_Reseller >> Cnt_res
Cnt_res >> Insert_transaction
Insert_transaction >> Cnt_trans
Cnt_trans >> Insert_venue
Insert_venue >> Cnt_vnu
Cnt_vnu >> Insert_CSV
Insert_CSV >> Cnt_csv
Cnt_csv >> Insert_XML

Insert_XML >> Cnt_xml
Cnt_xml >> trigger_dependent_dag