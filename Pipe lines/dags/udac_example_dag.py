from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries



default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Load events to redshit using custom Operator
stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket="udacity-dend",
        s3_key="log_data/2018/",
        file_type="JSON",
        time_format="epochmillisecs"
)

# Load songs to redshit using custom Operator
stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="udacity-dend",
        s3_key="song_data/A/",
        file_type="JSON"
)

# Load songplay fact table using custom Operator
load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        query = SqlQueries.songplay_table_insert
)

# Load user dim table using custom Operator
load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='user',
        query = SqlQueries.user_table_insert
)

# Load song dim table using custom Operator
load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='song',
        query = SqlQueries.song_table_insert
)

# Load artists dim table using custom Operator
load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artist',
        query = SqlQueries.artist_table_insert
)

# Load time dim table using custom Operator
load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        query = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tests=[
                [ 'songplays', 'SELECT COUNT(*) FROM songplays WHERE songplay_id IS NULL', 0 ]
                [ 'users'    , 'SELECT COUNT(*) FROM users WHERE user_id IS NULL', 0  ],
                [ 'songs'    , 'SELECT COUNT(*) FROM songs WHERE song_id IS NULL', 0],
                [ 'artists'  , 'SELECT COUNT(*) FROM artists WHERE artist_id IS NULL', 0],
                [ 'time_tab' ,'SELECT COUNT(*) FROM time_table WHERE start_time IS NULL', 0]
        ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# TASKS DEPENDENCIES

# Load into staging tables happens in parallel after the execution of start_operator
start_operator >>   stage_events_to_redshift
start_operator >>   stage_songs_to_redshift

# After the Exectution of Inserting into the 2 stagging tables, Loading into songplay fact table take place
stage_events_to_redshift >>  load_songplays_table
stage_songs_to_redshift  >>  load_songplays_table

# After loading into songplay fact table, Inserting into the 4 dim table happens in parallel
load_songplays_table  >>  load_user_dimension_table
load_songplays_table  >>  load_song_dimension_table
load_songplays_table  >>  load_artist_dimension_table
load_songplays_table  >>  load_time_dimension_table

# run quality operators starts execution after inserting into the 4 dim table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# After the quality check the end operator takes place
run_quality_checks >> end_operator 
