import sys
import os
import get_env_variables as gev
from CreateSpark import get_spark_object
from DataIngestion import load_files , display_df
from validate import get_current_date , technical_validation , get_dup_chk , col_dups
import logging.config


logging.config.fileConfig('Configurations/logging.config')
#logging.basicConfig(level=DEBUG)


def main():

    logging.info('starting the logging')
        #print(gev.header)
        #print(gev.rawsource)
    logging.info('Calling spark object')
    spark = get_spark_object(gev.envn , gev.appName)

    ############Read input data files
    logging.info('Reading Input data files')

    logging.info('Reading Customer Input data files')
    file_dir = f"{gev.rawsource}\olist_customers_dataset.csv"
    header = gev.header
    inferSchema = gev.inferSchema
    df_customer = load_files(spark , file_dir=file_dir , file_format='csv' , header=header , inferSchema=inferSchema)
    #display_df(df_customer, 'df_customer')
    error = []
    error.append(str(file_dir) + ',' + get_dup_chk(df_customer) )
    error.append(str(file_dir) + ',' + col_dups(df_customer , 'customer_id') )
    print(error)

    logging.info('Reading Order Input data files')
    file_dir = f"{gev.rawsource}\olist_orders_dataset.csv"
    header = gev.header
    inferSchema = gev.inferSchema
    df_orders = load_files(spark , file_dir=file_dir , file_format='csv' , header=header , inferSchema=inferSchema)
    #display_df(df_orders, 'df_orders')

    logging.info('Reading Order ITEM Input data files')
    file_dir = f"{gev.rawsource}\olist_order_items_dataset.csv"
    header = gev.header
    inferSchema = gev.inferSchema
    df_order_items = load_files(spark, file_dir=file_dir, file_format='csv', header=header, inferSchema=inferSchema)
    #display_df(df_order_items, 'df_order_items')

    logging.info('Reading Products Input data files')
    file_dir = f"{gev.rawsource}\olist_products_dataset.csv"
    header = gev.header
    inferSchema = gev.inferSchema
    df_products = load_files(spark, file_dir=file_dir, file_format='csv', header=header, inferSchema=inferSchema)
    #display_df(df_products, 'df_products')


if __name__ == '__main__':
    main()
    logging.info('Application Done')
