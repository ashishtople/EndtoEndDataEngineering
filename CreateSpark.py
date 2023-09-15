from pyspark.sql import SparkSession
import logging.config


logging.config.fileConfig('Configurations/logging.config')

logger = logging.getLogger('create_spark')
def get_spark_object(envn, appName):
    try:
        logger.info("get_spark_object started")
        if envn == 'Dev':
            master = 'local'
        else:
            master = 'Yarn'

        logger.info('master is {}'.format(master))

        spark = SparkSession.builder.master(master).appName(appName).getOrCreate()


    except Exception as exp:
        logger.error('An Error occured at Get spark object',str(exp))

        raise

    else:
        logger.info("spark object created")
    return spark