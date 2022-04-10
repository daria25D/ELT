from common import *
from datetime import datetime
import json
import random
import redis
import threading
import time


class GeneratorThread(threading.Thread):
    def __init__(self, log_file_name: str):
        super().__init__()
        self._stop = threading.Event()
        self.log_file_name = log_file_name
        # establish connection to Redis
        self.r = redis.Redis(host=HOST, port=6379, db=0)

    def generate_temperatures(self, batch_num=0):
        temperatures = []
        times = []
        for i in range(BATCH):
            temperature = random.randint(MIN_TEMP, MAX_TEMP)
            cur_time = datetime.now().strftime("%H:%M:%S")
            temperatures.append(temperature)
            times.append(cur_time)
            if not self.log_file.closed:
                self.log_file.write(f'{batch_num} {cur_time}: {temperature}\n')
            time.sleep(TEMP_PERIOD)
        # print(first_time, temperatures)
        self.send_data_to_redis(times[0], temperatures)

    def send_data_to_redis(self, key_time, temperatures):
        try:
            if not self.stopped():  # do not record last batch of data generated on interrupt ?
                print(key_time, *temperatures)
                self.r.set(key_time, json.dumps(temperatures)) # for better handling of decoding
        except redis.ConnectionError:
            print("Redis connection lost")

    def run(self):
        self.log_file = open(self.log_file_name, "w", 1)  # flush data
        batch_num = 1
        while not self.stopped():
            self.generate_temperatures(batch_num)
            batch_num += 1

    def stop_logging(self):
        self.log_file.close()

    def stop_redis_connection(self):
        self.r.close()

    def stopped(self):
        return self._stop.is_set()

    def stop(self):
        print("Terminating generation")
        self._stop.set()
        print("Terminating logging")
        self.stop_logging()
        self.stop_redis_connection()
