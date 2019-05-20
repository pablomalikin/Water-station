#Import
from socket import *
from datetime import datetime
import time

#Define

BUFF=1024
SVR_ADDR = "127.0.0.1", 43210
DELAY = 5
MIN_ID = 1
MAX_ID = 15
FILENAME = 'status.txt'
CONNECTED = "Connected to Server"

#Function

def fread(num) :
    curr_date = ''
    station_info = ''
    rflag = True
    with open(FILENAME,'r') as rfile :
        i = -1
        line = rfile.readline()
        while line :
            i += 1
            if i == num:
                return station_info
            else :
                station_info = ''
                curr_date = ''
                rflag = True
            for ch in line:
                if ch != ' ' :
                    curr_date += ch
                else:
                    station_info += curr_date+' '
                    if rflag :
                        curr_date=str(datetime.now().strftime('%d-%m-%Y %H:%M:%S'))
                        station_info += curr_date+' '
                        rflag = False
                    curr_date = ''
            station_info += str(int(curr_date))+' '
            line = rfile.readline()
    return station_info

def read(num) :
    station_info = fread(int(num))
    return station_info

#Main

while True:
    num_station = input("Enter station id [from {} to {}] : ".format(MIN_ID, MAX_ID))
    if num_station.isdigit():
        if MIN_ID <= int(num_station) <= MAX_ID:
            break
        else:
            print("You typed a wrong station id ")
            continue
    else:
        print("Not valid data,try again")

with socket(AF_INET, SOCK_STREAM) as s_client:
    s_client.bind(('', 0))
    try :
        s_client.connect(SVR_ADDR)
    except ConnectionRefusedError :
        print("Problem with connection")
        exit(1)

    while True :
        try :
            msg = s_client.recv(BUFF).decode()
        except Exception :
            continue
        print()
        station_info = read(num_station)

        if msg == "keep alive" :
            print(msg," ==> Delay {} sec".format(DELAY))
            time.sleep(DELAY)
            print()
        elif msg == "Water station id collision" :
            print(msg)
            break
        else :
            print(msg)
        if msg == CONNECTED or msg == "keep alive" :
            s_client.send(station_info.encode())
            print("Sending the data of the water station {}\n".format(station_info.split()[0]))
            station_info = ''

