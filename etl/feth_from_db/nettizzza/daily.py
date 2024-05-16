import os
import sys
from datetime import datetime

from etl.common import init_spark

current_date_str = sys.argv[1]
data_source = 'adc_daily'

default_job_cfg = {
    "executor.instances": 1,
    "executor.cores": 2,
    "executor.memory": '4g',
    "jars": ['/usr/lib/nzjdbc3.jar']
}

spark = init_spark.setup(
    job_cfg=default_job_cfg,
    script_name='airflow_fetch_for_{}_{}'.format(data_source, current_date_str)
)

output_dir = '/rawdata/adc_daily/date={}'.format(current_date_str)

sql = '''
(  
select to_date(file_date ,'yyyymmdd')  FILE_DATE,
trim(isdn,'+') ISDN, IMEI,TAC, BRAND, MODEL, OS_VENDOR, OS_NAME, OS_VERSION, LAST_CONFIGURATION  from bigdata.CDR_OWNER.ADC_DAILY_{}
where to_date(file_date ,'yyyymmdd') = to_date('{}','yyyy-mm-dd') 
) table_alias
'''.format(current_date_str[0:4],current_date_str)

df = spark.read. \
    format("jdbc") \
    .option("url", "jdbc:netezza://10.3.4.121:5480/BIGDATA") \
    .option("user", "admin") \
    .option("password", "bigmbf@2018") \
    .option("dbtable", sql) \
    .option("driver", "org.netezza.Driver") \
    .option("fetchsize", 100000) \
    .load()
df.write.mode("overwrite").option('header', 'true').csv(output_dir, compression="bzip2")

from etl.fetch_from_db.utils import check_fetch_result
check_fetch_result.check_has_result(spark.read.csv(output_dir))

# Write checksum file
log_count = df.count()
print(log_count)

# Put checksum to local
from etl.select.cic import put_check_sum_file_to_local_service
put_check_sum_file_to_local_service.process_checksum(log_count, data_source, current_date_str)