from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator, PythonOperator)
from helpers import SqlQueries
from dateutil.parser import parse
import operator

AWS_KEY = Variable.get('AWS_KEY')
AWS_SECRET = Variable.get('AWS_SECRET')

default_args = {
    'owner': 'student',
    'start_date': datetime(2015, 1, 1),
    'end_date': datetime(2015, 12, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'depends_on_past': True,
    'wait_for_downstream': True
}

dag = DAG('udacity-capstone-dag',
          default_args=default_args,
          description='Capstone project DAG',
          schedule_interval='0 0 1 1 *'
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_symbols_to_redshift = StageToRedshiftOperator(
    task_id="copy_symbols_task",
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    table="symbols",
    s3_bucket="bmi-bucket-udacity-us-west",
    s3_key="data/symbols"
)

stage_interest_rates_to_redshift = StageToRedshiftOperator(
    task_id="copy_interest_rates_task",
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    table="interest_rates",
    s3_bucket="bmi-bucket-udacity-us-west",
    s3_key="data/rates"
)

stage_news_to_redshift = StageToRedshiftOperator(
    task_id="copy_news_task",
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    table="news",
    s3_bucket="bmi-bucket-udacity-us-west",
    s3_key="data/news/year=2015"
)

stage_stocks_to_redshift = StageToRedshiftOperator(
    task_id="copy_stocks_task",
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    table="staging_stocks",
    s3_bucket="bmi-bucket-udacity-us-west",
    s3_key="data/stocks/year=2015/month=6"
)

create_time_dimension_table = LoadDimensionOperator(
    task_id="create_time_table_task",
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    sql=SqlQueries.time_table_insert,
    table="time",
    delete_before_insert=True
)

create_fact_table = LoadFactOperator(
    task_id="create_stock_analysis_fact_table",
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    sql=SqlQueries.stock_analysis_table_insert
)

end_operator = DummyOperator(task_id='Execution_done', dag=dag)

start_operator >> [stage_symbols_to_redshift, stage_interest_rates_to_redshift, stage_news_to_redshift,
                   stage_stocks_to_redshift]

[stage_symbols_to_redshift, stage_interest_rates_to_redshift, stage_news_to_redshift, stage_stocks_to_redshift] >> \
create_time_dimension_table

create_time_dimension_table >> create_fact_table

create_fact_table >> end_operator
