from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 sql,
                 table,
                 delete_before_insert=False,
                 *args, **kwargs):
        """
        Constructor for the LoadDimensionOperator, which creates
        dimension tables in a Postgres database using staging tables.

        Args:
            redshift_conn_id (str): name of the connection to Redshift
            sql (str): name of the SQL to be executed against the DB
            table (str): name of the dimension table
            delete_before_insert (bool): flag - if set to True, the
                dimension table is emptied before data is written to it
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.delete_before_insert = delete_before_insert

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_before_insert and self.table:
            self.log.info(f"Deleting data from {self.table}")
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Running sql query...")
        redshift.run(self.sql)
        self.log.info("...done.")
