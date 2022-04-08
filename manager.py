from common import GEN_LOG_FILE
from generator import GeneratorThread
from datetime import datetime
import random
import redis
import sys
import threading
import time


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
