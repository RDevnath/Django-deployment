import readParameter
from sqlalchemy.exc import SQLAlchemyError
import logging
from decouple import config

destination_table_1=config('DESTINATION_TABLE_1')
destination_table_2=config('DESTINATION_TABLE_2')

def read_data_from_kafkaTopic_1():
    logger=logging.getLogger()
    logger.setLevel(logging.DEBUG)

    kafka_consumer_1 = readParameter.getKafkaConsumer_1()
    sink_engine = readParameter.getSinkEngine()

    print('\nKafka Consumer 1 created to read data from kafka topic 1')
    print('\nsink database connected to insert data from kafka topic 1\n')
    count = 0
    try:
        for message in kafka_consumer_1:
            message = message.value
            res = sink_engine.execute("INSERT INTO {} VALUES ({},'{}','{}','{}','{}','{}')".format(destination_table_1,
                                                                                                   message['Emp_No'],
                                                                                                   message['Birth_Date'],
                                                                                                   message['First_Name'],
                                                                                                   message['Last_Name'],
                                                                                                   message['Gender'],
                                                                                                   message['Hire_Date']))
            count += 1
            if (count%1000 == 0):
                print('{} rows added to {} table in destination database'.format(count, destination_table_1))
            else:
                pass

    except KeyError as err:
        logger.error("Kafka consumer - Exception during connecting to broker - {}".format(err))
        raise err
    except SQLAlchemyError as err:
        logger.error(str(err.orig))
        raise err

def read_data_from_kafkaTopic_2():
    logger=logging.getLogger()
    logger.setLevel(logging.DEBUG)

    kafka_consumer_2 = readParameter.getKafkaConsumer_2()
    sink_engine = readParameter.getSinkEngine()

    print('\nKafka Consumer 2 created to read data from kafka topic 2')
    print('\nsink database connected to insert data from kafka topic 2\n')
    
    count = 0
    try:
        for message in kafka_consumer_2:
            message = message.value
            res = sink_engine.execute("INSERT INTO {} VALUES ({},{},'{}','{}')".format(destination_table_2,
                                                                                       message['Emp_No'],
                                                                                       message['Salary'],
                                                                                       message['From_Date'],
                                                                                       message['To_Date']))
            count += 1
            if (count%1000 == 0):
                print('{} rows added to {} table in destination database'.format(count,destination_table_2))
            else:
                pass

    except KeyError as err:
        logger.error("Kafka consumer - Exception during connecting to broker - {}".format(err))
        raise err
    except SQLAlchemyError as err:
        logger.error(str(err.orig))
        raise err
###END OF THE SCRIPT
