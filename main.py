import producer
import consumer
import validation
import logging
import time
from multiprocessing import Process

if __name__ == '__main__':
    
    st1 = time.time()
    logging.basicConfig(filename="Logfile.log",format='%(asctime)s - %(message)s',filemode='w')
    logger=logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    p1 = Process(target = producer.send_data_to_kafkaTopic_1)
    p2 = Process(target = consumer.read_data_from_kafkaTopic_1)

    p3 = Process(target = producer.send_data_to_KafkaTopic_2)
    p4 = Process(target = consumer.read_data_from_kafkaTopic_2)

    p5 = Process(target = validation.data_validation_1)
    p6 = Process(target = validation.data_validation_2)

    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p1.join()
    p2.join()
    p5.start()
    p3.join()
    p4.join()
    p6.start()
#END OF THE SCRIPT
