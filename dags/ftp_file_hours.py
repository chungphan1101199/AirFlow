from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

SCRIPT_HOME = '/apps/airflow'

args = {
    'owner': 'Tung',
}

data_types = (
     'mobile_marketing',
)
with DAG(
        dag_id='ftp_data_hourly',
        default_args=args,
        start_date=datetime(2020, 4, 26),
        schedule_interval='0 * * * *',
        tags=['ftp_data_hourly']
) as dag:
    ftp_task_list = []

    for index, data_type in enumerate(data_types):
        fetch_task_name = 'ftp_' + data_type
        ftp_file = BashOperator(
            task_id=fetch_task_name,
            bash_command = 'cd ' + SCRIPT_HOME + ' && /apps/anaconda2/envs/jupyter-py2/bin/python -m ' + 'cdr_downloader.scan '+ data_type#lay all nguon
        )

        ftp_task_list.append(ftp_file)

    start_task = DummyOperator(task_id='start')
    complete = DummyOperator(task_id='complete')

    start_task >> ftp_task_list >> complete

if __name__ == "__main__":
    dag.cli()