from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.postgres_operator import PostgresOperator
import logging


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table,
        # a query that will get the values to be inserted 
        self.query = query  

    def execute(self, context):
        
        logging.info("Load Table {}".format(self.table) )
        
        # SQL INSERT STATEMENT
        sql = "INSERT INTO {} {};".fomrat(self.table, self.query)
        
        # Check if Truncate is True
        if self.truncate_table:

            logging.info("Truncate Table {}".format(self.table) )
            sql = "TRUNCATE TABLE {}; \n".format(self.table) + sql

        # INSERT INTO FACT TABLE ON REDSHIFT
        load_fact_table = PostgresOperator(
        task_id="load_fact_table",
        postgres_conn_id= self.redshift_conn_id,
        sql = sql
        )
