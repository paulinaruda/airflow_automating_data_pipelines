from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend
from final_project_operators_and_sql.final_project_sql_statements import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_type="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_type = json_type
        
    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        metastoreBackend = MetastoreBackend()
        self.log.info(f"AWS Credentials ID: {self.aws_credentials_id}")
        aws_connection = metastoreBackend.get_connection(self.aws_credentials_id)
        self.log.info(f"AWS Connection Details: {aws_connection}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
    
        self.log.info("Connected to AWS")
        
        # Get the current database and available tables - the newly created ones were not showing in the UI
        query = """
            SELECT current_database() AS current_database,
            table_name
            FROM information_schema.tables
            WHERE table_schema = 'public';
        """
        result = redshift.get_records(query)

        self.log.info("Database and tables information:")
        for row in result:
            self.log.info(row)
            
        #redshift.run(f"use database dev")
        redshift.run(f"DROP TABLE IF EXISTS {self.table}")
        self.log.info(f"Dropped table {self.table}, (if existed).")
        self.log.info("Creating table")
        
        if 'events' in self.table:
           # redshift.run(self.staging_logs_table_create)
            redshift.run(SqlQueries.staging_logs_table_create)
        elif 'songs' in self.table:
            redshift.run(SqlQueries.staging_songs_table_create)
          #  redshift.run(self.staging_songs_table_create)

        self.log.info(f"Created table {self.table}")
        
        self.log.info(f"Starting to copy data from S3 bucket {self.s3_bucket} to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = SqlQueries.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.json_type
        )
        
        redshift.run(formatted_sql)
        self.log.info(f"Copied data into table {self.table}")

        



