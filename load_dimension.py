from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.decorators import apply_defaults
from final_project_operators_and_sql.final_project_sql_statements import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 append_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.append_only = append_only

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Connected to db")
        
        # Truncate table if not in append-only mode
        if not self.append_only:
            redshift.run(f"TRUNCATE TABLE {self.table}")
            self.log.info(f"Truncated table {self.table}")
        
        self.log.info("Creating tables")
        if 'time' in self.table:
            redshift.run(SqlQueries.time_table_create)
        elif 'artist' in self.table:
            redshift.run(SqlQueries.artist_table_create)   
        elif 'song' in self.table:
            redshift.run(SqlQueries.song_table_create)  
        elif 'users' in self.table:
            redshift.run(SqlQueries.user_table_create)  
        self.log.info(f"Created table {self.table}") 
        
        # Insert data into tables
        if not self.append_only:
            self.log.info("Inserting data into tables")
            if 'time' in self.table:
                redshift.run(SqlQueries.time_table_insert)
            elif 'artist' in self.table:
                redshift.run(SqlQueries.artist_table_insert)   
            elif 'song' in self.table:
                redshift.run(SqlQueries.song_table_insert)  
            elif 'users' in self.table:
                redshift.run(SqlQueries.user_table_insert) 
            self.log.info(f"Inserted data into the {self.table} table")

        # Append rows in tables
        if self.append_only:
            self.log.info("Appending rows in tables")
            if 'time' in self.table:
                redshift.run(SqlQueries.append_time_insert)
            elif 'artist' in self.table:
                redshift.run(SqlQueries.append_artist_insert)   
            elif 'song' in self.table:
                redshift.run(SqlQueries.append_song_insert)  
            elif 'users' in self.table:
                redshift.run(SqlQueries.append_user_insert) 
            self.log.info(f"Appended data into the {self.table} table")