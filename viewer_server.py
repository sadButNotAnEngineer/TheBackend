import mysql.connector
import time
import socket
import threading

myDataBase = mysql.connector.Connect(
    host = "localhost",
    user = "userWS",
    password = "password",
    database = "weatherstation"
)

def getFromDB(myDataBase, count):
    cursor = myDataBase.cursor()
    cursor.execute(f"SELECT UNIX_TIMESTAMP(time), DHT_RH, DHT_T, BMA_P, BMA_T FROM readings where time > DATE_SUB(NOW(), INTERVAL {count} HOUR);")
    # SELECT UNIX_TIMESTAMP(time), DHT_RH, DHT_T, BMA_P, BMA_T FROM (SELECT * FROM Readings ORDER BY id DESC LIMIT 70) AS sub ORDER BY id ASC;
    result = cursor.fetchall()
    cursor.close()
    myDataBase.commit()
    rval = str("")
    for r in result:
        rval+=(f"{r[0]},{r[1]},{r[2]},{r[3]},{r[4]}\n")
    return rval

connList = []
server_address = ('145.239.88.95', 20000)

def connectionHandler(conn, addr, count):
    while(conn):
        try:
            data = (getFromDB(myDataBase, count))
            print("sending data")
        except Exception as e: 
            print("getFromDB exception: ", e)
    
        try:
            conn.send(data.encode())
        except Exception as e: 
            print("send exception: ", e)

        try:
            request = conn.recv(1024).decode("utf-8")
            count = int(request.split(' ')[1])
        except Exception as e:
            print("recv exception: ", e)
                
    conn.close()


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
        if("get" in data):
            count = int(data.split(' ')[1])
            thread = threading.Thread(target = connectionHandler, args = (conn, addr, count))
            thread.start()
            connList.append(thread)
        else:
            print("wrong Header: ", data)