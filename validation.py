import readParameter
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from decouple import config

destination_table_1=config('DESTINATION_TABLE_1')
destination_table_2=config('DESTINATION_TABLE_2')

def data_validation_1():

    print('\nValidating data extracted using query 1 and the data stored from kafka consumer 1')

    source_engine = readParameter.getSourceEngine()
    sink_engine = readParameter.getSinkEngine()

    print('\n source database engine created to evaluate producer data sent at kafka topic 1')
    print('\n sink database engine created to evaluate consumer data received from kafka topic 1')

    fd = open('query1.sql', 'r')
    sql_command_1 = fd.read()
    fd.close()

    try:
        df1 = pd.read_sql(sql_command_1,source_engine)
        df2 = pd.read_sql("select * from {};".format(destination_table_1), sink_engine)

        df_diff = pd.concat([df1, df2]).drop_duplicates(keep=False)
        
        if (len(df_diff) > 0):
            print("\nMISMATCH in records for source and destination tables")
            print("\nSource table shape = {} and destination table shape = {}".format(df1.shape, df2.shape))
        else:
            print("\nSource and Destination Table MATCHED")
            print("\nSource table shape = {} and destination table shape = {}".format(df1.shape, df2.shape))
    except SQLAlchemyError as err:
        logger.error(str(err.orig))
        raise err

def data_validation_2():
    
    print('\nValidating data extracted using query 2 and the data stored from kafka consumer 2')

    source_engine = readParameter.getSourceEngine()
    sink_engine = readParameter.getSinkEngine()

    print('\n source database engine created to evaluate producer data sent at kafka topic 2')
    print('\n sink database engine created to evaluate consumer data received from kafka topic 2')

    fd = open('query2.sql', 'r')
    sql_command_2 = fd.read()
    fd.close()

    try:
        df1 = pd.read_sql(sql_command_2,source_engine)
        df2 = pd.read_sql("select * from {};".format(destination_table_2), sink_engine)

        df_diff = pd.concat([df1, df2]).drop_duplicates(keep=False)

        if (len(df_diff) > 0):
            print("\nMISMATCH in records for source and destination tables")
            print("\nSource table shape = {} and destination table shape = {}".format(df1.shape, df2.shape))
        else:
            print("\nSource and Destination Table MATCHED")
            print("\nSource table shape = {} and destination table shape = {}".format(df1.shape, df2.shape))
    except SQLAlchemyError as err:
        logger.error(str(err.orig))
        raise err
