from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 *args, **kwargs):
        """
        Constructor for the LoadFactOperator, which creates
        fact tables in a Postgres database using staging tables.

        Args:
            redshift_conn_id (str): name of the connection to Redshift
            sql (str): name of the SQL to be executed against the DB
            table (str): name of the fact table
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Running sql query...")
        redshift.run(self.sql)
        self.log.info("...done.")
