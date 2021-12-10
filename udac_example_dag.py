from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
AWS_KEY = os.environ.get('')
AWS_SECRET = os.environ.get('')

#run /opt/airflow/start.sh to begin
# default_args is complete
default_args = {
    'owner': 'Mike',
    'start_date': datetime(2021, 11, 1),
    'end_date': datetime(2021, 12, 8),
    'catchup': False,
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

# dag is complete
dag = DAG('dend-dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1
        )

# need to check this operator
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# staging events to redshift is complete
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_table_events",
    #s3_path="s3://udacity-dend",
    s3_bucket="log_data",
    s3_key="log_data/{ execution_date.year }/{ execution_date.month } / { ds }-events.json",
)


# staging songs to redshift is complete
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_table_songs", 
    #s3_path="s3://udacity-dend",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/" # got help from https://knowledge.udacity.com/questions/300829
    
)

# I think this is ok
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays_table",
    redshift_conn_id="redshift",
    table_columns="(playid, start_time, userid, level, songid, artitsid, sessionid, location, user_agent)",
    sql_select=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="user_dimension_table",
    redshift_conn_id="redshift",
    table_columns="(userid, firstname, lastname, gender, level)",
    sql_select=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="song_dimension_table",
    redshift_conn_id="redshift",
    table_columns="(song_id, title, artist_id, year, duration)",
    sql_select=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artist_dimension_table",
    redshift_conn_id="redshift",
    table_columns="(artist_id, artist_name, artist_location, artist_latitude, artist_longitude)",
    sql_select=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time_dimension_table",
    redshift_conn_id="redshift",
    table_columns="(start_time, hour, day, week, month, year, dayofweek)",
    sql_select=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    quality_checks=[{"checksql": SqlQueries.data_quality_check_user_nulls, "expected": 0},
                    {"checksql": SqlQueries.data_quality_check_song_nulls, "expected": 0},
                    {"checksql": SqlQueries.data_quality_check_artist_nulls, "expected": 0}]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator