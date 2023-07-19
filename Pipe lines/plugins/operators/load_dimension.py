from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.postgres_operator import PostgresOperator
import logging

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 query='',
                 truncate_table = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table  
        #a query that will get the values to be inserted
        self.query = query  
        self.truncate_table = truncate_table

    def execute(self, context):

        logging.info("Load Table {}".format(self.table) )
        
        # SQL INSERT STATEMENT
        sql = "INSERT INTO {} {};".format(self.table, self.query)
        
        # Check if truncate is True
        if self.truncate_table:

            logging.info("Truncate Table {}".format(self.table) )
            sql = "TRUNCATE TABLE {}; \n".format(self.table) + sql
        
        #INSERT INTO DIM TABLE ON REDSHIFT
        load_fact_table = PostgresOperator(
        task_id="load_dim_table",
        postgres_conn_id= self.redshift_conn_id,
        sql = sql
         )