from common import DB, FLUSH_PERIOD, HOST
import json
import redis
import sqlite3
import threading
import time


class BrokerThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self._stop = threading.Event()
        self.r = redis.Redis(host=HOST, port=6379, db=0, decode_responses=True)
        try:
            self.con = sqlite3.connect(DB)
            self.drop_old_db()
            self.create_db()
        except sqlite3.Error as e:
            print(f'Fail on init: {e}')
        finally:
            if self.con:
                self.con.close()

    def drop_old_db(self):
        cursor = self.con.cursor()
        cursor.execute("DROP TABLE IF EXISTS temps")
        self.con.commit()

    def create_db(self):
        cursor = self.con.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS temps ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "time TEXT NOT NULL,"
            "temp1 INTEGER NOT NULL,"
            "temp2 INTEGER NOT NULL,"
            "temp3 INTEGER NOT NULL,"
            "temp4 INTEGER NOT NULL,"
            "temp5 INTEGER NOT NULL,"
            "temp6 INTEGER NOT NULL,"
            "temp7 INTEGER NOT NULL,"
            "temp8 INTEGER NOT NULL,"
            "temp9 INTEGER NOT NULL,"
            "temp10 INTEGER NOT NULL)"
        )
        self.con.commit()

    def push_data_to_db(self, key, values):
        cursor = self.con.cursor()
        data = (key, *values)
        cursor.execute(
            '''INSERT INTO temps(time,temp1,temp2,temp3,temp4,temp5,temp6,temp7,temp8,temp9,temp10)
              VALUES(?,?,?,?,?,?,?,?,?,?,?)''', data
        )
        self.con.commit()

    def run(self):
        while not self.stopped():
            time.sleep(FLUSH_PERIOD)
            print('Start transferring data')
            data = {}
            for key in self.r.scan_iter('*'):
                print(key)
                r_list = json.loads(self.r.get(key))
                data[key] = r_list
                self.r.delete(key)
            print(data)
            try:
                self.con = sqlite3.connect(DB)
                self.con.isolation_level = 'EXCLUSIVE'
                self.con.execute('BEGIN EXCLUSIVE')
                with self.con:
                    for key in sorted(data.keys()):
                        self.push_data_to_db(key, data[key])
            except sqlite3.Error as e:
                print(f'Fail at run: {e}')
            finally:
                if self.con:
                    self.con.close()

    def stop_redis_connection(self):
        self.r.close()

    def stopped(self):
        return self._stop.is_set()

    def stop(self):
        print("Terminating data transfer")
        self._stop.set()
        self.stop_redis_connection()
