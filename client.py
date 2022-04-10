import sqlite3
from common import DB


def main():
    try:
        con = sqlite3.connect(DB)
        cursor = con.cursor()
        sql = '''SELECT id, time, MAX(temp)
                 FROM
                 (
                     SELECT id, time, MAX(temp1, temp2, temp3) AS temp FROM test
                 )'''
        cursor.execute(sql)
        rows = cursor.fetchall()
        for r in rows:
            print(r)
    except sqlite3.Error as e:
        print(f'Fail at client: {e}')
    finally:
        if con:
            con.close()


if __name__ == '__main__':
    main()
