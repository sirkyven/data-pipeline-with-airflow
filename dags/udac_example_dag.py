from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries



default_args = {
    'owner': 'Venkatesh Gangisetti',
    'start_date': datetime(2020, 1, 3),
    'email': ['venkateshgangisetti@icloud.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          catchup=False
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag
    redshift_conn_id="redshift",
    aws_credentials_id="aws_auth",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    delimiter=",",
    ignore_headers=1,
    create_stmt = SqlQueries.create_table_staging_events
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag
    redshift_conn_id="redshift",
    aws_credentials_id="aws_auth",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    delimiter=",",
    ignore_headers=1,
    create_stmt = SqlQueries.create_table_staging_songs
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
