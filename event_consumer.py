from kafka import KafkaConsumer
from datetime import datetime
import os
import json


KAFKA_TOPIC='monitor'

def main():
    print('starting consumer')
    kafka_address = os.environ['KAFKA_ADDRESS']
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=[kafka_address])
    for message in consumer:
        print ("topic=%s\npartition=%d\noffset=%d\nkey=%s\nvalue=%s\n\n" %
               (message.topic,
                message.partition,
                message.offset,
                message.key,
                json.dumps(json.loads(message.value), indent=4)))

if __name__ == "__main__":
    main()
