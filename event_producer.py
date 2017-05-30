from kafka import KafkaProducer
from datetime import datetime
import psutil
import os
import json
import time

KAFKA_TOPIC='monitor'

def generate_stats(interval=1):
    stats = dict()

    stats['time'] = datetime.now().isoformat()
    
    _cpu_stats = psutil.cpu_times_percent(interval)
    cpu_stats = dict()
    cpu_stats['user'] = _cpu_stats.user
    cpu_stats['system'] = _cpu_stats.system
    cpu_stats['iowait'] = _cpu_stats.iowait
    cpu_stats['idle'] = _cpu_stats.idle
    stats['cpu'] = cpu_stats

    _virt_mem = psutil.virtual_memory()
    mem_stats = dict()
    mem_stats['total'] = _virt_mem.total
    mem_stats['available'] = _virt_mem.available
    mem_stats['percent'] = _virt_mem.percent
    _swap = psutil.swap_memory()
    mem_stats['swap'] = dict()
    mem_stats['swap']['total'] = _swap.total
    mem_stats['swap']['used'] = _swap.used
    mem_stats['swap']['percent'] = _swap.percent
    stats['mem'] = mem_stats

    _disk_usage = psutil.disk_usage('/')
    disk_stats = dict()
    disk_stats['total'] = _disk_usage.total
    disk_stats['used'] = _disk_usage.used
    disk_stats['percentage'] = _disk_usage.percent
    stats['disk'] = disk_stats

    return stats

def main():
    print('starting producer')
    kafka_address = os.environ['KAFKA_ADDRESS']
    producer = KafkaProducer(bootstrap_servers=[kafka_address])
    while True:
        stats = generate_stats(os.environ['CPU_INTERVAL'] if 'CPU_INTERVAL' in os.environ else 1)
        out = json.dumps(stats).encode()
        print('sending: %s' % stats['time'])
        future = producer.send(KAFKA_TOPIC, out)
        record_metadata = future.get(timeout=10)

if __name__ == "__main__":
    main()
