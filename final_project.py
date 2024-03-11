from datetime import timedelta
import pendulum

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from final_project_operators_and_sql.stage_redshift import StageToRedshiftOperator
from final_project_operators_and_sql.load_fact import LoadFactOperator
from final_project_operators_and_sql.load_dimension import LoadDimensionOperator
from final_project_operators_and_sql.data_quality import DataQualityOperator

 
default_args = {
    'owner': 'udacity',
    'depends_on_past': True,
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *' 
)

def final_project():

    start_operator = EmptyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket="udacity-dend",
        s3_key="log_data",
        json_type="s3://udacity-dend/log_json_path.json",
        )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="udacity-dend",
        s3_key="song_data",
        json_type="auto",
        )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_cridentials",
        append_only=True
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_cridentials",
        table="users",
        append_only=True)

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_cridentials",
        table="song",
        append_only=False)

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_cridentials",
        table="artist",
        append_only=True)

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_cridentials",
        table="time",
        append_only=True)

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_cridentials"
    )
    
    end_operator = EmptyOperator(task_id='End_execution')

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table
    load_time_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_user_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()