from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

SCRIPT_HOME = '/apps/airflow'
current_date_str = '{{ ( (execution_date).in_timezone("Asia/Ho_Chi_Minh") ).strftime("%Y-%m-%d") }}'

args = {
    'owner': 'Tung',
}

data_types =  (
              'msc.msc_SMT',
             # 'msc.msc_OG',
             # 'msc.msc_IC',
             # 'msc.msc_SMO',
           
              )
partner = 'dstk'
with DAG(
        dag_id='xport_data_cap2',
        default_args=args,
        start_date=datetime(2019, 9, 1, 0),
        end_date=datetime(2021, 2, 1, 0),
        schedule_interval='0 12 * * *',
        # schedule_interval=None,
        max_active_runs=90,
) as dag:
    
    select_task_list = []
    putfile_to_local_task_list = []
    for type in data_types:
        select_q = ''
        put_q = ''
        if '.' in type:
            select_q = type.replace('.','.select_')
            put_q = type.replace('.','.put_file_to_local_')
        else:
            select_q = 'select_'+ type
            put_q = 'put_file_to_local_'+ type
            
        select_task_name = select_q
        select_task = BashOperator(
            task_id=select_task_name,
            bash_command='cd {}  && /apps/anaconda2/envs/jupyter-py2/bin/python -m  etl.select.{}.{} {}'.format(
                SCRIPT_HOME, partner, select_q,
                current_date_str),
            pool='special_pool',
        )
        select_task_list.append(select_task)

        put_file_to_local_task_name = put_q
        put_file_to_local_task = BashOperator(
            task_id=put_file_to_local_task_name,
            bash_command='cd {}  && /apps/anaconda2/envs/jupyter-py2/bin/python -m  etl.select.{}.{} {}'.format(
                SCRIPT_HOME, partner, put_q,
                current_date_str),
            pool='put_file_to_local',
        )
        putfile_to_local_task_list.append(put_file_to_local_task)
        select_task >> put_file_to_local_task

    start_task = DummyOperator(task_id='start')
    start_task >> select_task_list 
    complete_task_all = DummyOperator(task_id='complete_all')
    start_task >> complete_task_all
    
    
if __name__ == "__main__":
    dag.cli()
