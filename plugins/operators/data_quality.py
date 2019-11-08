from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 sql,
                 result,
                 test_operator,
                 *args, **kwargs):
        """
        Constructor for the DataQuality Operator, which performs
        data quality checks against a Postgres database.

        Args:
            redshift_conn_id (str): name of the connection to Redshift
            sql (str): name of the SQL to be executed against the DB
            result (int): expected result
            test_operator (operator): using this parameter, the relation
                between the test result and expected result can be
                determined. For example, operator.ne would check if the
                test result is unequal to the expected result.
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.result = result
        self.test_operator = test_operator

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        test_result = redshift.get_first(self.sql)

        # Test result is a tuple, extract only the first element, which should be a number
        if self.test_operator(test_result[0], self.result):
            self.log.info(f"Data quality check passed!")
        else:
            raise ValueError(f"Test did not pass. Test result: {test_result}, expected: {self.result}.")
