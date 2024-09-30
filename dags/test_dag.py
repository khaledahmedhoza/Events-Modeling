from cgitb import lookup
import datetime
import logging

from numpy import rec

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
#from airflow.contrib.hooks.aws_hook import AwsHook

def load_data_to_postgres(*args, **kwargs):
    #log_path = "'/home/weblog{}.log'".format( {kwargs['execution_date']} )
    log_path = "'/home/weblog.log'"
    request = "copy prod.weblogs (client_ip , user_identity , user_name , request_date , request_type , \
                                request , status , response_size , referer , user_agent )\
                                from {}  DELIMITER '&'".format(log_path)
                                
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run(request)

def check_logs_inserted(*args, **kwargs):
    table = kwargs["params"]["table"]
    #exec_dt = {kwargs['execution_date']} 
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    records = pg_hook.get_records(f"select count(*) from {table} where (request_date::date)::varchar = (({kwargs['ds']})::varchar) ")
    if records is None or len(records)< 1:
        logging.error("No logs inserted")
        raise ValueError("No logs inserted")

dag = DAG(
    'B2B_Ecommerce',
    start_date=datetime.datetime(2022,7,1,0,0,0,0),
    schedule_interval="@monthly"
)

creat_log_table = PostgresOperator(
    task_id = 'create_table',
    postgres_conn_id='postgres',
    sql='create table if not exists prod.weblogs (client_ip text, user_identity text, user_name text, request_date timestamp, request_type text, request text, status text, response_size bigint, referer text, user_agent text)',
    dag=dag
)

copy_log_table = PythonOperator(
    task_id = 'copy_table',
    python_callable=load_data_to_postgres,
    dag=dag,
    provide_context=True
)

check_weblogs = PythonOperator(
    task_id = 'check_inserted_logs',
    dag=dag,
    python_callable=check_logs_inserted,
    provide_context=True,
    params={
        'table':'prod.weblogs'
    }
)

creat_table_customer = PostgresOperator(
    task_id = 'create_table_customer',
    postgres_conn_id='postgres',
    sql='create table if not exists dmart.customers (customer_key serial primary key, cuit_identifier bigint,\
         customer_name text, customer_DOB timestamp)',
    dag=dag
)

insert_into_customer = PostgresOperator(
    task_id = 'insert_into_customer',
    postgres_conn_id='postgres',
    sql='insert into dmart.customers (cuit_identifier ,customer_name , customer_DOB ) \
        select cust_document_no, customer_name, date_of_birth from prod."Customers" \
        where (created_at::date)::varchar = ({{ds}})::varchar',
    dag=dag
)


creat_table_product = PostgresOperator(
    task_id = 'creat_table_product',
    postgres_conn_id='postgres',
    sql='create table if not exists dmart.products (product_key serial primary key, product_src_id bigint,\
         product_src_type text)',
    dag=dag
)

insert_into_product_seller = PostgresOperator(
    task_id = 'insert_into_product_seller',
    postgres_conn_id='postgres',
    sql='insert into dmart.products (product_src_id ,product_src_type ) \
        select prod_id, \'Seller\' from prod."Seller_products" where (created_at::date)::varchar = ({{ds}})::varchar',
    dag=dag
)

insert_into_product_supplier = PostgresOperator(
    task_id = 'insert_into_product_supplier',
    postgres_conn_id='postgres',
    sql='insert into dmart.products (product_src_id ,product_src_type ) \
        select prod_id, \'Supplier\' from prod."Supplier_products" where (created_at::date)::varchar = ({{ds}})::varchar',
    dag=dag
)

creat_table_company = PostgresOperator(
    task_id = 'creat_table_company',
    postgres_conn_id='postgres',
    sql='create table if not exists dmart.company (company_key serial primary key, company_src_identifier bigint,\
         company_name text, company_type text)',
    dag=dag
)

insert_into_company = PostgresOperator(
    task_id = 'insert_into_company',
    postgres_conn_id='postgres',
    sql='insert into dmart.company (company_src_identifier ,company_name , company_type) \
        select cuit_identifier, company_name, company_type from prod."Companies" where (created_at::date)::varchar = ({{ds}})::varchar',
    dag=dag
)

creat_table_order_request = PostgresOperator(
    task_id = 'creat_table_order_request',
    postgres_conn_id='postgres',
    sql='create table if not exists dmart.order_request (req_key serial primary key, req_src_id text, browser text, country text)',
    dag=dag
)

insert_into_order_request = PostgresOperator(
    task_id = 'insert_into_order_request',
    postgres_conn_id='postgres',
    sql='insert into dmart.order_request (req_src_id, browser , country ) \
        select user_name||request_date::varchar ,user_agent, ( select l.country_name from geolite.blocks b\
                            inner join geolite.location l on b.geoname_id = l.geoname_id\
                            where network >>= (client_ip)::inet ) country \
        from prod."weblogs" where (request_date::date)::varchar = ({{ds}})::varchar',
    dag=dag
)


creat_table_order_fct = PostgresOperator(
    task_id = 'creat_table_order_fct',
    postgres_conn_id='postgres',
    sql='create table if not exists dmart.order_fct (order_key serial primary key, order_date timestamp,\
         product_key bigint, end_user_type text, end_user_key bigint, order_request_key bigint, order_src_id bigint,\
             order_item_src_id bigint, line_item_qty bigint, line_paid_amt double precision)',
    dag=dag
)

insert_into_order_fct = PostgresOperator(
    task_id = 'insert_into_order_fct',
    postgres_conn_id='postgres',
    sql='insert into dmart.order_fct (order_date ,product_key , end_user_type , end_user_key ,\
             order_request_key , order_src_id ,order_item_src_id , line_item_qty , line_paid_amt ) \
        select o.created_at, \
            (select product_key from dmart.products p where p.product_src_id = COALESCE(sp.prod_id ,slp.prod_id ) ),\
            o.end_user_type, case when o.end_user_type=\'Customer\' then (select customer_key from dmart.customers where cuit_identifier=o.customer_id ) else (select company_key from dmart.company where company_src_identifier=o.customer_id ) end,\
            (select req_key from dmart.order_request orq where orq.req_src_id= COALESCE(c.Customer_name,cp.company_name)||o.created_at ),\
            o.order_id, oi.order_item_id, oi.quantity, oi.total_paid\
            from prod."orders" o\
                inner join prod."order_items" oi on o.order_id = oi.order_id \
                left join prod."Customers" c on o.customer_id = c.Cust_Document_no and o.order_src_type=\'Seller\' \
                left join prod."Companies" cp on o.customer_id = cp.CUIT_Identifier and o.order_src_type=\'Supplier\' \
                left join prod."Supplier_products" sp on oi.item_id = sp.prod_id and o.order_src_type=\'Supplier\' \
                left join prod."Seller_products" slp on oi.item_id = slp.prod_id and o.order_src_type=\'Seller\' \
                where (o.created_at::date)::varchar = ({{ds}})::varchar',
    dag=dag
)
#grep Generated logs/nifi-app*log
#usr : 9951f9ca-14cb-4625-821b-bc9674176729
#pwd: n9BzdmmJ+6c2AyCsnZ3YJ8VUb2vj2Eho

creat_log_table >> copy_log_table
copy_log_table >> check_weblogs

check_weblogs >> creat_table_order_request
check_weblogs >> creat_table_customer
creat_table_order_request >> insert_into_order_request

creat_table_customer >> insert_into_customer
creat_table_customer >> creat_table_company

creat_table_company >> insert_into_company
creat_table_company >> creat_table_product

creat_table_product >> insert_into_product_supplier
insert_into_product_supplier >> insert_into_product_seller

insert_into_product_seller >> creat_table_order_fct
creat_table_order_fct >> insert_into_order_fct
