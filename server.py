#IMPORTS

import sqlite3 as sql
from socket import *
from datetime import datetime
import time

BUFF = 1024
IP = "127.0.0.1"
PORT = 43210
DELAY = 0.001
START = 0
INSERT = 0
UPDATE = 1
CONNECTED = "Connected to Server"

#DATA SQL

db_file = "data.db"

sql_create_table_water_station = """
CREATE TABLE IF NOT EXISTS station_status (
	station_id INT,
	last_date TEXT,
	alarm1 INT,
	alarm2 INT,
	PRIMARY KEY(station_id)
);
"""

sql_select_all_stations = """
SELECT rowid, *
FROM station_status;
"""

sql_insert_station = """
INSERT INTO STATION_STATUS VALUES
(?, ?, ?, ?);
"""

sql_select_status_station_id = """
SELECT rowid, *
FROM station_status 
WHERE station_id == ?;
"""

sql_delete_station = """
DELETE FROM station_status 
WHERE station_id = ?;
"""

sql_update = """
UPDATE station_status 
SET last_date=?,alarm1=?,alarm2=?
WHERE station_id = ?;
"""

setdefaulttimeout(DELAY)

with sql.connect(db_file) as data_base:
    db = data_base.cursor()
    db.execute(sql_create_table_water_station)

    with socket(AF_INET, SOCK_STREAM) as s_server:
        s_server.bind((IP, PORT))
        s_server.listen(1)
        print("Waiting for water station")
        print("listening on {}:{}.".format(IP, PORT))
        water_st=[]

        while True:
            try :
                conn,addr = s_server.accept()
            except OSError :
                pass
            else :
                station_id = START
                flag = START
                option = -1
                msg = CONNECTED
                water_st.append([conn, addr[1], station_id, flag, option, msg])
                new_conn = conn
                new_port = new_conn.getpeername()[1]
                print("A new station was connected. Port : {}".format(new_port))
                conn.send(msg.encode())

            for i,(conn, port, station_id, flag, option, msg) in enumerate(water_st) :
                try:
                    num_station = conn.recv(BUFF)
                except OSError:
                    continue

                if num_station == b'' :
                    print("A station {} was disconnected. Port : {}".format(station_id,port))
                    water_st.pop(i)
                    break

                num_station = num_station.decode().split()
                station_id = num_station[0]
                last_date = num_station[1] + ' ' + num_station[2]
                alarm1 = num_station[3]
                alarm2 = num_station[4]

                water_st[i][2] = station_id
                print(i, port, station_id, flag)

                if not flag :
                    flag = UPDATE
                    water_st[i][3] = flag
                    option = INSERT
                else :
                    option = UPDATE

                if option == 0 :
                    cur = db.execute(sql_select_status_station_id, (station_id,))
                    bcur = bool(list(cur))
                    if not bcur:
                        db.execute(sql_insert_station, (station_id, last_date, alarm1, alarm2))
                    else:
                        print("Station {} exist".format(station_id))
                        msg = "Water station id collision"
                        conn.send(msg.encode())
                        fail = True
                        water_st.pop(i)
                        print("A Port : {} was disconnected".format(port))
                elif option == 1 :
                    cur = db.execute(sql_update, (last_date, alarm1, alarm2, station_id))
                else:
                    pass
                data_base.commit()

                if msg == CONNECTED :
                    msg = "keep alive"
                    water_st[i][5] = msg
                conn.send(msg.encode())
