from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 quality_checks="",
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id,
        self.quality_checks=quality_checks

    def execute(self, context):
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Quality check sequence initiating')
        
        num_error=0
        failures=[] # inspired by https://knowledge.udacity.com/questions/631168
        
        for i in self.quality_checks:
            sql_query=i.get('checksql')
            exp_res=check.get('expected')
            
            res=redshift.get_records(sql_query)[0]
            
            if exp_res != res[0]:
                num_error += 1
                failures.append(sql_check)
                
            if num_error > 0:
                self.log.info('The below tests failed quality checks:')
                self.log.info(failures)
                raise ValueError('Failed data quality')
            else:
                self.log.info('Passed data quality')
        