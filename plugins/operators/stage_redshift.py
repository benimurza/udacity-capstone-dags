from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from dateutil.parser import parse
from airflow.models import Variable


class StageToRedshiftOperator(BaseOperator):
    YEAR_PLACEHOLDER = "<YEAR>"
    MONTH_PLACEHOLDER = "<MONTH>"
    DAY_PLACEHOLDER = "<DAY>"

    template_fields = ("s3_key",)
    ui_color = '#358140'

    copy_csv = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 aws_key,
                 aws_secret,
                 table,
                 s3_bucket,
                 s3_key: str,
                 use_timestamp_templating=False,
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.access_key = aws_key
        self.secret_key = aws_secret
        self.use_timestamp_templating = use_timestamp_templating

    def execute(self, context):
        if self.use_timestamp_templating:
            execution_date = context['ds']
            year = parse(execution_date).year
            month = parse(execution_date).month
            day = parse(execution_date).day
            self.s3_key = self.s3_key \
                .replace(self.YEAR_PLACEHOLDER, str(year)) \
                .replace(self.MONTH_PLACEHOLDER, str(month)) \
                .replace(self.DAY_PLACEHOLDER, str(day))

            self.log.info(f"Using templated key: {self.s3_key}")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_csv.format(
            self.table,
            s3_path,
            self.access_key,
            self.secret_key,
            self.ignore_headers,
            self.delimiter
        )
        redshift.run(formatted_sql)
