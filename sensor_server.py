import mysql.connector
import time
import socket
import threading

myDataBase = mysql.connector.Connect(
    host = "localhost",
    user = "root",
    password = "password",
    database = "WeatherStation"
)

def sendToDB(myDataBase, DHT_RH, DHT_T, BMA_P, BMA_T):
    cursor = myDataBase.cursor()
    query = "INSERT INTO Readings (time, DHT_RH, DHT_T, BMA_P, BMA_T) VALUES (FROM_UNIXTIME(%s), %s, %s, %s, %s)"

    values = (time.time(), DHT_RH, DHT_T, BMA_P, BMA_T)
    cursor.execute(query, values)

    myDataBase.commit()
    cursor.close()

connList = []
server_address = ('192.168.100.96', 10000)

def connectionHandler(conn, addr):

    while True:
        try:
            data = conn.recv(1024)
        except Exception as e: 
            print("recv exception: ", e)
            break

        if not data: 
            print("Client disconnected:", addr)
            break
        msg = data.decode("utf-8")
        vals = msg.split(' ')
        sendToDB(myDataBase, float(vals[0]), float(vals[1]), float(vals[3]), float(vals[2])/100)                

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as serverSocket:
    serverSocket.bind(server_address)

    while True:
        serverSocket.listen(10)

        conn, addr = serverSocket.accept()
        print("new client connected: ", conn)
        try:
            data = conn.recv(1024).decode("utf-8") 
        except Exception as e: 
            print("recv exception: ", e)
            continue
        if(data == "RH t BMA_t p"):
            thread = threading.Thread(target = connectionHandler, args = (conn, addr))
            thread.start()
            connList.append(thread)
        else:
            print("wrong Header: ", data)
        