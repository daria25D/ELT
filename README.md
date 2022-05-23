# ELT
ELT pipeline with python, SQLite and redis

## How it works

When manager runs, it creates threads for generator and broker.
Generator thread creates data (here: temperature data) and sends it to redis "data lake".
Broker extracts data from redis and loads it to SQLite database.

Client can access this database and find specific data entry (in this case: highest temperature).
