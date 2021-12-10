from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    # got help from https://knowledge.udacity.com/questions/483503  
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id,
        self.aws_credentials_id=aws_credentials_id,
        self.table=table,
        self.s3_bucket=s3_bucket,
        self.s3_key=s3_key

    def execute(self, context):
        self.log.info(f"Loading data from {self.s3_bucket}/{self.s3_key}")
        redshiftconn=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3hook=AwkHook(self.aws_credentials_id)
        aws_creds=s3hook.get_credentials(region_name=self.s3_region)
        s3_path=f"s3://{self.s3_bucket}/{self.s3_key}"