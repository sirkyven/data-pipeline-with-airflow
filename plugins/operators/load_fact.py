from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                #  sql,
                #  conn_id
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # self.sql = sql
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        # postgres_hook = PostgresHook(self.conn_id)
        # postgres_hook.run(self.sql)
        