import sys
from datetime import timedelta, datetime

from etl.common import init_spark

current_date_str = sys.argv[1]
data_source = 'action_audit'

default_job_cfg = {
    "executor.instances": 1,
    "executor.cores": 2,
    "executor.memory": '4g',
    "jars": ['/usr/lib/ojdbc6.jar']
}

spark = init_spark.setup(
    job_cfg=default_job_cfg,
    script_name='airflow_fetch_for_{}_{}'.format(data_source, current_date_str)
)

month_str = datetime.strptime(current_date_str, '%Y-%m-%d').strftime('%Y%m')
next_date_str = (datetime.strptime(current_date_str, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

output_dir = '/rawdata/action_audit/date={}'.format(current_date_str)
sql = '''
( 
   SELECT 1 as sub_type,  
        action_audit_id,
       shop_code,
       issue_datetime,
       sub_id,
       action_id,
       user_name,
       pc,
       reason_id,
       description,
       status,
       free_sim,
       emp_code,
       ''||valid valid ,
         app_id,
        channel
  FROM mc_action_audit@gold
 WHERE     issue_datetime >= to_date('{}','yyyy-MM-dd')
       AND issue_datetime < to_date('{}','yyyy-MM-dd')
UNION
SELECT 
         0 as sub_type, 
        action_audit_id,
       shop_code,
       issue_datetime,
       pk_id sub_id,
       action_id,
       user_name,
       pc,
       reason_id,
       description,
       NULL AS status,
       free_sim,
       emp_code,
      ''||valid valid ,
        app_id,
       channel
  FROM action_audit@gold
 WHERE     issue_datetime >= to_date('{}','yyyy-MM-dd')
       AND issue_datetime < to_date('{}','yyyy-MM-dd')
 ) table_alias
'''.format(current_date_str, next_date_str, current_date_str, next_date_str)

df = spark.read. \
    format("jdbc") \
    .option("url", "jdbc:oracle:thin:@10.50.8.22:1521:REPORT1") \
    .option("user", "fintech") \
    .option("password", "fintech") \
    .option("dbtable", sql) \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .option("fetchsize", 100000) \
    .load()

log_count = df.count()
print(log_count)


df.write.mode("overwrite").option('header', 'true').csv(output_dir, compression="bzip2")



