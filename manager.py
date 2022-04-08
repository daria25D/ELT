from common import *
from datetime import datetime
import random
import redis
import sys
import threading
import time


class GeneratorThread(threading.Thread):
    def __init__(self, log_file_name: str):
        super().__init__()
        self._stop = threading.Event()
        self.log_file_name = log_file_name
        # establish connection to Redis
        self.r = redis.Redis(host=HOST, port=6379, db=0)
        self.redis_id = REDIS_QUEUE
        if self.r.exists(self.redis_id):  # some leftovers
            self.r.delete(self.redis_id)

    def run(self):
        self.running = 1
        self.log_file = open(self.log_file_name, "w", 1) # flush data
        while not self.stopped():
            temperatures = [random.randint(MIN_TEMP, MAX_TEMP)]
            first_time = datetime.now().strftime("%H:%M:%S")
            if not self.log_file.closed:
                self.log_file.write(f'{first_time}: {temperatures[0]}\n')
            time.sleep(TEMP_PERIOD)
            for i in range(1, BATCH):
                temperature = random.randint(MIN_TEMP, MAX_TEMP)
                cur_time = datetime.now().strftime("%H:%M:%S")
                temperatures.append(temperature)
                if not self.log_file.closed:
                    self.log_file.write(f'{cur_time}: {temperature}\n')
                time.sleep(TEMP_PERIOD)
            # print(first_time, temperatures)
            self.send_data_to_redis(first_time, temperatures)

    def send_data_to_redis(self, key_time, temperatures):
        try:
            self.r.lpush(key_time, *temperatures)
        except redis.ConnectionError:
            print("Redis connection lost")

    def stop_logging(self):
        self.log_file.close()

    def stop_redis_connection(self):
        self.r.close()

    def stopped(self):
        return self._stop.isSet()

    def stop(self):
        print("Terminating generation")
        self._stop.set()
        print("Terminating logging")
        self.stop_logging()
        self.stop_redis_connection()


def wait_thread_end(t: GeneratorThread):
    try:
        t.join()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            sys.exit(1)
    finally:
        t.stop()


def main():
    gen_thread = GeneratorThread(GEN_LOG_FILE)
    gen_thread.start()
    wait_thread_end(gen_thread)


if __name__ == '__main__':
    main()
