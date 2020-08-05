from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                  redshift_conn_id="",
                  sql_insert_query="",
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        
        self.redshift_conn_id=redshift_conn_id
        self.sql_insert_query= sql_insert_query

    def execute(self, context):
        self.log.info('Load Fact Operator not implemented yet')
        
        redshift_hook=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift_hook.run(self.sql_insert_query)

        self.log.info(f'success: Load Fact')