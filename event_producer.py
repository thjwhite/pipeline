from kafka import KafkaProducer
from datetime import datetime
import os
import json
import time


KAFKA_TOPIC='monitor'

def main():
    print('starting producer')
    kafka_address = os.environ['KAFKA_ADDRESS']
    producer = KafkaProducer(bootstrap_servers=[kafka_address])
    while True:
        data = dict()
        data['time'] = datetime.now().isoformat()
        j = json.dumps(data).encode()
        print('sending: %s' % j)
        future = producer.send(KAFKA_TOPIC, j)
        record_metadata = future.get(timeout=10)
        time.sleep(1)

if __name__ == "__main__":
    main()
