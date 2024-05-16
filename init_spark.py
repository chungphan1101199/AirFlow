"""Spark session validate and initialize"""

import os
import atexit
import warnings
import socket

import py4j
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession


def _spark_default_configuration():
    return {
        'paths': {
            'spark_home': '/usr/hdp/current/spark2-client',
            'java_home': '/usr/java/default',
            'python_path': '/apps/anaconda2/envs/jupyter-py2/bin/python'
        },
        'spark_config': {
            'master': 'yarn',
            'driver.cores': 1,
            'driver.memory': '1g',
            'executor.instances': 2,
            'executor.cores': 4,
            'executor.memory': '4g',
            'port.maxRetries': 50,
        },
    }


def setup(job_cfg=None, script_name='unnamed'):
    """ Validate and initialize spark session if need

        Parameters
        ----------
        spark_paths: spark's paths
        job_cfg: spark's job configuration
        script_name: spark job's name for monitoring
        spark: passed spark session

        Returns
        -------
        an initialized spark session
    """
    print('in init spark here')

    config = _spark_default_configuration()

    if job_cfg:
        config['spark_config'].update(job_cfg)

    print(config)

    pyspark_submit_args_custom = ''
    pyspark_submit_args_custom = pyspark_submit_args_custom + ' '.join(
        '--conf spark.{}={}'.format(key, value) for key, value in config['spark_config'].items() if key != 'jars'
    )

    jars = config['spark_config'].get('jars', '')
    if jars:
        print(jars)
        pyspark_submit_args_custom = pyspark_submit_args_custom + ' --jars ' + \
                                     ','.join('{}'.format(jar) for jar in jars)

    print('pyspark submit arg: {}'.format(pyspark_submit_args_custom))

    # TODO consider make "SPARK_EXECUTOR_URI", 'PYSPARK_SUBMIT_ARGS' ... constants # pylint: disable=fixme
    os.environ['SPARK_HOME'] = config['paths'].get('spark_home')
    os.environ['JAVA_HOME'] = config['paths'].get('java_home')
    os.environ['PYSPARK_PYTHON'] = config['paths'].get('python_path')

    os.environ['PYSPARK_SUBMIT_ARGS'] = (
            pyspark_submit_args_custom +
            ' --name "{}" pyspark-shell'.format(script_name)
    )

    if os.environ.get("SPARK_EXECUTOR_URI"):
        SparkContext.setSystemProperty("spark.executor.uri", os.environ["SPARK_EXECUTOR_URI"])
    # pylint: disable=W0212
    SparkContext._ensure_initialized()
    try:
        # Try to access HiveConf, it will raise exception if Hive is not added
        conf = SparkConf()
        if conf.get('spark.sql.catalogImplementation', 'hive').lower() == 'hive':
            SparkContext._jvm.org.apache.hadoop.hive.conf.HiveConf()
            spark = SparkSession.builder \
                .enableHiveSupport() \
                .getOrCreate()
        else:
            spark = SparkSession.builder.getOrCreate()
    except py4j.protocol.Py4JError:
        if conf.get('spark.sql.catalogImplementation', '').lower() == 'hive':
            warnings.warn("Fall back to non-hive support because failing to access HiveConf, "
                          "please make sure you build spark with hive")
        spark = SparkSession.builder.getOrCreate()
    except TypeError:
        if conf.get('spark.sql.catalogImplementation', '').lower() == 'hive':
            warnings.warn("Fall back to non-hive support because failing to access HiveConf, "
                          "please make sure you build spark with hive")
        spark = SparkSession.builder.getOrCreate()
    # pylint: disable=W0108
    atexit.register(lambda: spark.sparkContext.stop())
    return spark
