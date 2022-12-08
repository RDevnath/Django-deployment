import readParameter
import pandas as pd
from decouple import config
from sqlalchemy.exc import SQLAlchemyError
import logging

kafka_topic_1=config('KAFKA_TOPIC_1')
kafka_topic_2=config('KAFKA_TOPIC_2')

def send_data_to_kafkaTopic_1():
    logger=logging.getLogger()
    logger.setLevel(logging.DEBUG)

    source_engine = readParameter.getSourceEngine()
    kafka_producer = readParameter.getKafkaProducer()
    
    print('source database connected to extract data passed for Query 1')
    print('\nkafka producer created to send data to topic 1\n')

    fd = open('query1.sql', 'r')
    sql_command = fd.read()
    fd.close()
    count = 0
    try:
        df = pd.read_sql(sql_command,source_engine)
        for i in range(df.shape[0]):
            my_dict = {'Emp_No' : int(df.iloc[i]['emp_no']),
                       'Birth_Date' : str(df.iloc[i]['birth_date']),
                       'First_Name' : df.iloc[i]['first_name'],
                       'Last_Name' : df.iloc[i]['last_name'],
                       'Gender' : df.iloc[i]['gender'],
                       'Hire_Date' : str(df.iloc[i]['hire_date'])}
            
            kafka_producer.send(kafka_topic_1, my_dict)
            count += 1
            if (count%1000 == 0):
                print('{} records sent to kafka topic 1 from employees table from employee database'.format(count))
            else:
                pass
    
    except KeyError as ke:
        logger.error("KafkaLogsProducer error sending log to Kafka: %s", ke)
        raise ke
    except SQLAlchemyError as err:
        logger.error(str(err.orig))
        raise err

def send_data_to_KafkaTopic_2():
    logger=logging.getLogger()
    logger.setLevel(logging.DEBUG)

    source_engine = readParameter.getSourceEngine()
    kafka_producer = readParameter.getKafkaProducer()

    print('\nsource database connected to extract data passed for Query 2')
    print('\nkafka producer created to send data to topic 2\n')

    fd = open('query2.sql', 'r')
    sql_command = fd.read()
    fd.close()
    count = 0
    try:
        df = pd.read_sql(sql_command,source_engine)
        for i in range(df.shape[0]):
            my_dict = {'Emp_No' : int(df.iloc[i]['emp_no']),
                       'Salary' : int(df.iloc[i]['salary']),
                       'From_Date' : str(df.iloc[i]['from_date']),
                       'To_Date' : str(df.iloc[i]['to_date'])}
            kafka_producer.send(kafka_topic_2, my_dict)
            count += 1
            if (count%1000 == 0):
                print('{} records sent to kafka topic 2 from salaries table from employee database'.format(count))
            else:
                pass
    
    except KeyError as ke:
        logger.error("KafkaLogsProducer error sending log to Kafka: %s", ke)
        raise ke
    except SQLAlchemyError as err:
        logger.error(str(err.orig))
        raise err
###END OF THE SCRIPT
