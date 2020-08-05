from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    copy_sql = """
        {}
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 sql_query = "",
                 file_type="",
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.file_type = file_type
        self.aws_credentials_id = aws_credentials_id
        self.sql_query = sql_query

    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Upserting data to Redshift")
        formatted_sql = LoadFactOperator.copy_sql.format(
            sql_query = self.sql_query,
            access_key = credentials.access_key,
            secret_key = credentials.secret_key,
            file_type = self.file_type
        )
        redshift.run(formatted_sql)
