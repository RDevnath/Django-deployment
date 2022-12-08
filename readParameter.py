import logging
from decouple import config
from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import dumps
from json import loads
from sqlalchemy import create_engine
from sqlalchemy import insert
from sqlalchemy.exc import SQLAlchemyError

source_postgre_host=config('SOURCE_POSTGRE_HOST')
source_postgre_user=config('SOURCE_POSTGRE_USER')
source_postgre_password=config('SOURCE_POSTGRE_PASSWORD')
source_postgre_database=config('SOURCE_POSTGRE_DATABASE')
source_postgre_port=config('SOURCE_POSTGRE_PORT')

destination_postgre_host=config('DESTINATION_POSTGRE_HOST')
destination_postgre_user=config('DESTINATION_POSTGRE_USER')
destination_postgre_password=config('DESTINATION_POSTGRE_PASSWORD')
destination_postgre_database=config('DESTINATION_POSTGRE_DATABASE')
destination_postgre_port=config('DESTINATION_POSTGRE_PORT')

source_mysql_host=config('SOURCE_MYSQL_HOST')
source_mysql_user=config('SOURCE_MYSQL_USER')
source_mysql_password=config('SOURCE_MYSQL_PASSWORD')
source_mysql_database=config('SOURCE_MYSQL_DATABASE')
source_mysql_port=config('SOURCE_MYSQL_PORT')

destination_mysql_host=config('DESTINATION_MYSQL_HOST')
destination_mysql_user=config('DESTINATION_MYSQL_USER')
destination_mysql_password=config('DESTINATION_MYSQL_PASSWORD')
destination_mysql_database=config('DESTINATION_MYSQL_DATABASE')
destination_mysql_port=config('DESTINATION_MYSQL_PORT')

source_server=config('SOURCE_DATABASE_SERVER')
destination_server=config('DESTINATION_DATABASE_SERVER')

bootstrap_servers=config('BOOTSTRAP_SERVERS')

kafka_topic_1=config('KAFKA_TOPIC_1')
kafka_topic_2=config('KAFKA_TOPIC_2')


def getSourceEngine():
    logger=logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.info("creating source db connection")
    try:
        if(source_server.lower() == 'postgres'):
            source_engine = create_engine("postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}".format(source_postgre_user,
                                                                                             source_postgre_password,
                                                                                             source_postgre_host,
                                                                                             source_postgre_port,
                                                                                             source_postgre_database))
            logger.info("source DB connection done")
            return source_engine
        elif(source_server.lower() == 'mysql'):
            source_engine = create_engine("mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(source_mysql_user,
                                                                                       source_mysql_password,
                                                                                       source_mysql_host,
                                                                                       source_mysql_port,
                                                                                       source_mysql_database))
            logger.info("source DB connection done")
            return source_engine
    except SQLAlchemyError as err:
        logger.error(str(err.orig))
        raise err

def getSinkEngine():
    logger=logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.info("creating destination db connection")
    try:
        if(destination_server.lower() == 'postgres'):
            sink_engine = create_engine("postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}".format(destination_postgre_user,
                                                                                           destination_postgre_password,
                                                                                           destination_postgre_host,
                                                                                           destination_postgre_port,
                                                                                           destination_postgre_database))
            logger.info("Destination DB connection done")
            return sink_engine
        elif(destination_server.lower() == 'mysql'):
            sink_engine = create_engine("mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(destination_mysql_user,
                                                                                     destination_mysql_password,
                                                                                     destination_mysql_host,
                                                                                     destination_mysql_port,
                                                                                     destination_mysql_database))
            logger.info("Destination DB connection done")
            return sink_engine
    except SQLAlchemyError as err:
        logger.error(str(err.orig))
        raise err

def getKafkaProducer():
    logger=logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.info("Creating a Kafka Producer")
    try:
        kafka_producer = KafkaProducer(bootstrap_servers=[bootstrap_servers], 
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))

        logger.info("Successfully Created the Kafka Producer")
        return kafka_producer
    except KafkaTimeoutError as kte:
        logger.error("KafkaLogsProducer timeout sending log to Kafka: %s", kte)
        raise kte
    except KeyError as ke:
        logger.error("KafkaLogsProducer error sending log to Kafka: %s", ke)
        raise ke
    except Exception as e:
        logger.exception("KafkaLogsProducer exception sending log to Kafka: %s", e)
        raise e

def getKafkaConsumer_1():
    logger=logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.info("Creating a Kafka Consumer to read from topic 1")
    try:
        kafka_consumer_1 = KafkaConsumer(kafka_topic_1,
                                       bootstrap_servers=[bootstrap_servers],
                                       auto_offset_reset='earliest',
                                       enable_auto_commit=True,
                                       consumer_timeout_ms=1000,
                                       value_deserializer=lambda x: loads(x.decode('utf-8')))
        logger.info("Successfully Created the Kafka consumer for topic 1")
        return kafka_consumer_1
    except KeyError as err:
        logger.error("Kafka consumer - Exception during connecting to broker - {}".format(err))
        raise err

def getKafkaConsumer_2():
    logger=logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.info("Creating a Kafka Consumer to read from topic 2")
    try:
        kafka_consumer_2 = KafkaConsumer(kafka_topic_2,
                                       bootstrap_servers=[bootstrap_servers],
                                       auto_offset_reset='earliest',
                                       enable_auto_commit=True,
                                       consumer_timeout_ms=1000,
                                       value_deserializer=lambda x: loads(x.decode('utf-8')))
        logger.info("Successfully Created the Kafka consumer for topic 2")
        return kafka_consumer_2
    except KeyError as err:
        logger.error("Kafka consumer - Exception during connecting to broker - {}".format(err))
        raise err
##END OF THE SCRIPT
