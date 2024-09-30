from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import datetime
import logging

dag = DAG(
    'B2B_Event_Insert_Into_Dwh',
    start_date=datetime.datetime(2022,7,1,0,0,0,0),
    schedule_interval="@monthly"
)

Insert_venue = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/transform/InsertvenueDwh.py' ,
		conn_id= 'spark_local', 
		task_id='Insert_venue_dwh', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':' \'{{run_id}}\'+\'{{ts}}\' ' }
		)

Insert_Csv_trans = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/transform/InsertCsvTransactionDwh.py' ,
		conn_id= 'spark_local', 
		task_id='Insert_csv_transactions_dwh', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':' \'{{run_id}}\'+\'{{ts}}\' ' }
		)

Insert_Cust = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/transform/InsertCustomerDwh.py' ,
		conn_id= 'spark_local', 
		task_id='Insert_customer_dwh', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':' \'{{run_id}}\'+\'{{ts}}\' ' }
		)

Insert_Event = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/transform/InsertEventDwh.py' ,
		conn_id= 'spark_local', 
		task_id='Insert_event_dwh', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':' \'{{run_id}}\'+\'{{ts}}\' ' }
		)

Insert_Loc = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/transform/InsertLocationDwh.py' ,
		conn_id= 'spark_local', 
		task_id='Insert_location_dwh', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':' \'{{run_id}}\'+\'{{ts}}\' ' }
		)

Insert_Pltfrm_trans = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/transform/InsertPltfrmTransactionDwh.py' ,
		conn_id= 'spark_local', 
		task_id='Insert_transactions_dwh', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':' \'{{run_id}}\'+\'{{ts}}\' ' }
		)

Insert_Res = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/transform/InsertResellerDwh.py' ,
		conn_id= 'spark_local', 
		task_id='Insert_reseller_dwh', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':' \'{{run_id}}\'+\'{{ts}}\' ' }
		)

Insert_Xml_trans = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/transform/InsertXmlTransactionDwh.py' ,
		conn_id= 'spark_local', 
		task_id='Insert_xml_transactions_dwh', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':' \'{{run_id}}\'+\'{{ts}}\' ' }
		)		

Cnt_csv_trans = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/transform/Validating_src_dest/dwh_records_cnt_trans.py' ,
		conn_id= 'spark_local', 
		task_id='count_csv_transactions', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'', 'src_tbl':'spreadsheet_transaction','dest_tbl':'Transaction', 'dup_col':'TransId','src_type':'Csv_3rdparthy_platfrom'  }
		)

Cnt_xml_trans = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/transform/Validating_src_dest/dwh_records_cnt_trans.py' ,
		conn_id= 'spark_local', 
		task_id='count_xml_transactions', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'', 'src_tbl':'xml_transaction','dest_tbl':'Transaction', 'dup_col':'TransId','src_type':'Xml_3rdparty_platfrom'  }
		)

Cnt_trans = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/transform/Validating_src_dest/dwh_records_cnt_trans.py' ,
		conn_id= 'spark_local', 
		task_id='count_transactions', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'', 'src_tbl':'evnt_pltfrm_transaction','dest_tbl':'Transaction', 'dup_col':'TransId','src_type':'platfrom'  }
		)

Cnt_cust = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/transform/Validating_src_dest/dwh_records_cnt.py' ,
		conn_id= 'spark_local', 
		task_id='count_customers', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'', 'src_tbl':'evnt_pltfrm_customer','dest_tbl':'Customer', 'dup_col':'CustId' }
		)

Cnt_evnt = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/transform/Validating_src_dest/dwh_records_cnt.py' ,
		conn_id= 'spark_local', 
		task_id='count_events', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'', 'src_tbl':'evnt_pltfrm_events','dest_tbl':'Event', 'dup_col':'EventId' }
		)

Cnt_loc = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/transform/Validating_src_dest/dwh_records_cnt.py' ,
		conn_id= 'spark_local', 
		task_id='count_location', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'', 'src_tbl':'evnt_pltfrm_location','dest_tbl':'Location', 'dup_col':'LocId' }
		)

Cnt_res = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/transform/Validating_src_dest/dwh_records_cnt.py' ,
		conn_id= 'spark_local', 
		task_id='count_reseller', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'', 'src_tbl':'evnt_pltfrm_reseller','dest_tbl':'Reseller', 'dup_col':'ResellerId' }
		)

Cnt_vnu = SparkSubmitOperator(
		application ='/opt/airflow/dags/spark/transform/Validating_src_dest/dwh_records_cnt.py' ,
		conn_id= 'spark_local', 
		task_id='count_venue', 
		driver_class_path='/opt/airflow/dags/postgresql-42.4.2.jar',
		jars='/opt/airflow/dags/postgresql-42.4.2.jar',
		dag=dag,
		env_vars={'run_date':'{{ds}}','BatchId':'\'{{run_id}}\'+\'{{ts}}\'', 'src_tbl':'evnt_pltfrm_venue','dest_tbl':'Venue', 'dup_col':'VenueId' }
		)

Insert_Loc >> Cnt_loc
Cnt_loc >> Insert_Cust
Insert_Cust >> Cnt_cust
Cnt_cust >> Insert_venue
Insert_venue >> Cnt_vnu
Cnt_vnu >> Insert_Event
Insert_Event >> Cnt_evnt
Cnt_evnt >> Insert_Res
Insert_Res >> Cnt_res
Cnt_res >> Insert_Pltfrm_trans
Insert_Pltfrm_trans >> Cnt_trans
Cnt_trans >> Insert_Csv_trans
Insert_Csv_trans >> Cnt_csv_trans
Cnt_csv_trans >> Insert_Xml_trans
Insert_Xml_trans >> Cnt_xml_trans