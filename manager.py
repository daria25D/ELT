from common import GEN_LOG_FILE, HOST
from generator import GeneratorThread
from broker import BrokerThread
from typing import Union
from datetime import datetime
import random
import redis
import sys
import threading
import time


def wait_thread_end(t: Union[GeneratorThread, BrokerThread]):
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


def delete_old_data():
    r = redis.Redis(host=HOST, port=6379, db=0)
    for k in r.scan_iter('*'):
        r.delete(k)
    r.close()


def main():
    delete_old_data()
    gen_thread = GeneratorThread(GEN_LOG_FILE)
    gen_thread.start()
    broker_thread = BrokerThread()
    broker_thread.start()
    wait_thread_end(gen_thread)
    wait_thread_end(broker_thread)


if __name__ == '__main__':
    main()
