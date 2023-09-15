import logging.config
import os
import get_env_variables as gev
from pyspark.sql.functions import col

logging.config.fileConfig('Configurations/logging.config')
loggers = logging.getLogger('validate')

def technical_validation(df):
    dq_result = []
    def empty_chk(df):
        loggers.warning('Check if the dataframe is empty')
        if df.rdd.isEmpty():
            dq_result.append('Dataframe created is empty')
        else:
            dq_result.append('Dataframe created is NOT empty')

        return dq_result

    dq_results = empty_chk(df)
    return print(dq_results)

def get_dup_chk(df):
    #error = []
    row_lvl = df.groupBy(df.columns).count().filter(col("count") > 1)
    #row_lvl_dups = row_lvl[[col , 'count']]
    if row_lvl.rdd.isEmpty():
        error = 'No Row Level duplicates in dataframe'
    else:
        error = 'Row Level duplicates Present in dataframe'

    return str(error)


def col_dups(df,column):
    col_dups = df.groupBy(column).count().filter(col("count") > 1)
    if col_dups.rdd.isEmpty():
        error = 'No PK Column duplicates in dataframe'
    else:
        error = 'PK Column level duplicates present in dataframe'
    return str(error)

def get_current_date(spark):
    try:
        loggers.warning('started the get current date method')
        output = spark.sql("""select current_date""")
        loggers.warning('Validating current date' + str(output.collect()))
    except Exception as e:
        loggers.error('An Error Occured in finding current date' , str(e))

        raise

    else:
        loggers.warning('Validation Done')
