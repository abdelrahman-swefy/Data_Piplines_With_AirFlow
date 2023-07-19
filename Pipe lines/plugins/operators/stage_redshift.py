from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend
import logging

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 file_type="",
                 ignore_headers=1,
                 time_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.time_format = time_format
        self.file_type = file_type

    def execute(self, context):

        # connect to redshift using Postgres Hooks Operator
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        
        # copy data from S3 to Redshift
        logging.info("Copying data from S3 to Redshift")
        
        # determine the S3 path
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        
        # copy query
        qry = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                FORMAT AS {} 
                IGNOREHEADER {}
                DELIMITER '{}'
                TIMEFORMAT AS '{}';
        """.format( self.table, self.s3_path, self.aws_connection.login, self.aws_connection.password, self.file_type, self.ignore_headers,self.delimiter, self.time_format)

        redshift.run(qry)




