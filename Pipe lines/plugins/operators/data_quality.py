from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tests = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        # tests is a 2D list each list stores the table name, sql test to be run, expected result of that test
        self.tests = tests 

    def execute(self, context):
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # iterate over the tests dictionary the key is the sql statement and value is the expected result
        for test in self.tests:
            
            # table name
            table = test[0]
            
            # sql test
            sql = test[1]

            # expected result
            expected_result = test[2]
            
            records = redshift_hook.get_records(sql)
            
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f"Data quality check failed. {table} returned no results")
                raise ValueError(f"Data quality check failed. {table} returned no results")
            
            result = records[0][0]
            
            if result != expected_result:
                self.log.error(f"Data quality check failed. {table} contained {result} records, expected result is {expected_result}")
                raise ValueError(f"Data quality check failed. {table} contained {result} records,  expected result is {expected_result}")