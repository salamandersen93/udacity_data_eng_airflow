from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id= "",
                 table="",
                 sql_query="",
                 append_only=False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id,
        self.table=table,
        self.sql_query=sql_query,
        self.append_only=append_only

    def execute(self, context):
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"scratching data from {self.table}")
        
        if self.append_only==True: # appending only
            sql_query=self.sql_query.format(self.table) # referenced https://knowledge.udacity.com/questions/214641 for help
            redshift.run(sql_query)
        else: # scratching and reloading the data
            scratch_query=f"DELETE FROM {self.table}" 
            redshift.run(scratch_query)
            sql_query=self.sql_query.format(self.table)
            redshift.run(sql_query)
        self.log_info(f"Loaded data into {self.table}")