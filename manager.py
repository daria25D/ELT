from common import GEN_LOG_FILE, HOST
from generator import GeneratorThread
from broker import BrokerThread
import redis
import sys


def wait_threads_end(g: GeneratorThread, b: BrokerThread):
    try:
        g.join()
        b.join()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            sys.exit(1)
    finally:
        g.stop()
        b.stop()


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
    wait_threads_end(gen_thread, broker_thread)


if __name__ == '__main__':
    main()
