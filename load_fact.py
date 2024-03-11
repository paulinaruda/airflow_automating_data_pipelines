from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.secrets.metastore import MetastoreBackend
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from final_project_operators_and_sql.final_project_sql_statements import SqlQueries

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_key="",
                 append_only="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_key = s3_key
        self.append_only = append_only
        
    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
    
        if self.append_only == False:
            self.log.info("Connected to db")
            redshift.run("Drop table if exists songplays")
            self.log.info("Dropped table songplays if existed")
            redshift.run(SqlQueries.songplay_table_create) 
            self.log.info("Constructed the fact table")
            redshift.run(SqlQueries.songplay_table_insert) 
            self.log.info("Inserted data into the fact table")

        if self.append_only == True:
            self.log.info("Attempting to append missing rows")
            redshift.run(SqlQueries.append_songplays_insert) 
            self.log.info("Appended missing rows")

