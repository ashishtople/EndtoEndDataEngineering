import logging.config

logging.config.fileConfig('Configurations/logging.config')
logger = logging.getLogger('ingest')

def load_files(spark , file_dir , file_format , header , inferSchema):
    try:
        logger.warning('load files method started')

        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)

        elif file_format == 'csv':
            df = spark.read.format(file_format).option("header", header).option("inferSchema", inferSchema).load(file_dir)

    except Exception as e:
        logger.error("Error Occured at load files  " , str(e))
        raise
    else:
        logger.warning('dataframe created successfully which is of {}'.format(file_format))

    return df

def display_df(df,dfName):
    df_show = df.show()
    return df_show