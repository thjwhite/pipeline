from kafka import KafkaProducer
from datetime import datetime
import psutil
import os
import json
import time

KAFKA_TOPIC='monitor'

def compute_cpu(cpu_times_0):
    cpu_times_1 = psutil.cpu_times()
    tot_time = sum(cpu_times_1) - sum(cpu_times_0)
    cpu_stats = dict()
    if tot_time == 0:
        cpu_stats['user'] = 0
        cpu_stats['system'] = 0
        cpu_stats['iowait'] = 0
        cpu_stats['idle'] = 0
        return cpu_stats
    cpu_stats['user'] = 100 * (cpu_times_1.user - cpu_times_0.user) / tot_time
    cpu_stats['system'] = 100 * (cpu_times_1.system - cpu_times_0.system) / tot_time
    cpu_stats['iowait'] = 100 * (cpu_times_1.iowait - cpu_times_0.iowait) / tot_time
    cpu_stats['idle'] = 100 * (cpu_times_1.idle - cpu_times_0.idle) / tot_time
    return cpu_stats

def compute_mem():
    virt_mem = psutil.virtual_memory()
    mem_stats = dict()
    mem_stats['total'] = virt_mem.total
    mem_stats['available'] = virt_mem.available
    mem_stats['percent'] = virt_mem.percent
    swap = psutil.swap_memory()
    mem_stats['swap'] = dict()
    mem_stats['swap']['total'] = swap.total
    mem_stats['swap']['used'] = swap.used
    mem_stats['swap']['percent'] = swap.percent
    return mem_stats

def compute_storage_usage():
    disk_usage = psutil.disk_usage('/')
    disk_stats = dict()
    disk_stats['total'] = disk_usage.total
    disk_stats['used'] = disk_usage.used
    disk_stats['percentage'] = disk_usage.percent
    return disk_stats

def compute_disk_counters(disk_counters_0, interval):
    disk_counters_1 = psutil.disk_io_counters()
    disk_counter_stats = dict()
    disk_counter_stats['read_per_sec'] = (disk_counters_1.read_count - disk_counters_0.read_count) / interval
    disk_counter_stats['write_per_sec'] = (disk_counters_1.write_count - disk_counters_0.write_count) / interval
    disk_counter_stats['read_bytes_per_sec'] = (disk_counters_1.read_bytes - disk_counters_0.read_bytes) / interval
    disk_counter_stats['write_bytes_per_sec'] = (disk_counters_1.write_bytes - disk_counters_0.write_bytes) / interval
    return disk_counter_stats
    
def generate_stats(interval):
    cpu_times_0 = psutil.cpu_times()
    disk_counters_0 = psutil.disk_io_counters()
    time.sleep(interval)

    stats = dict()
    stats['cpu'] = compute_cpu(cpu_times_0)
    stats['mem'] = compute_mem()
    stats['disk'] = compute_storage_usage()
    stats['disk_counters'] = compute_disk_counters(disk_counters_0, interval)
    stats['time'] = datetime.now().isoformat()
    return stats

def main():
    print('starting producer')
    kafka_address = os.environ['KAFKA_ADDRESS']
    producer = KafkaProducer(bootstrap_servers=[kafka_address])
    while True:
        stats = generate_stats(os.environ['CPU_INTERVAL'] if 'CPU_INTERVAL' in os.environ else 5)
        out = json.dumps(stats).encode()
        print('sending: %s' % stats['time'])
        future = producer.send(KAFKA_TOPIC, out)
        record_metadata = future.get(timeout=10)

if __name__ == "__main__":
    main()
