
import socket
import threading
import time

server_address = ("10.17.6.5", 9801)
index1 = 0
index2 = 0
index3 = 0
my_ip = "10.194.27.67"

received = [0 for i in range(1001)]
data_received = []
list_of_lines = []
shared_dict = {}
total_received = 0
line_lock = threading.Lock()

def receive_lines_from_server():
    global shared_dict,list_of_lines,total_received,received
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(server_address)

    while(True):
        if(total_received>=1000):
            break
        message = "SENDLINE\n"
        client_socket.send(message.encode())
        received_message = b''
        while True:  # Continue receiving until a newline is received
            data_chunk = client_socket.recv(1024)
            if not data_chunk:  # No more data to receive
                break
            received_message += data_chunk  # Append the received chunk
            # Check if the received data ends with a newline character
            if received_message.endswith(b'\n'):
                break
        # Convert the binary data to a string and decode
        data = received_message.decode()
        split_data = data.split('\n')
        line_no = int(split_data[0])
        line = split_data[1]
        if line_no!=-1:
            if not received[line_no]:
                data_received.append(received_message)
                list_of_lines.append((line_no,line))
                # print("recvd from gawd = ", total_received)
                total_received+=1
                # print("recieved from server",total_received)
                shared_dict[line_no]=line
                received[line_no]=1
    
    client_socket.close()

def receive_line_from_slave1():
    global shared_dict,list_of_lines,total_received,received,data_received
    print("listening on thread1")
    host = my_ip
    port = 12002
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.bind((host, port))
    client_socket.listen()
    conn, address = client_socket.accept()
    # print("RECEIVE Connection from: " + str(address))
    while(True):
        if(total_received>=1000):
            break
        received_message = b''
        global_res = []
        while True:  # Continue receiving until a newline is received
            if(total_received>=1000):break
            prev=""
            data_chunk = conn.recv(4096)
            spl = data_chunk.decode().split('\n')
            res = []
            spl_size = len(spl)
            for i in range(spl_size):
                if (i==0 and spl[i].isnumeric() == False):
                    prev = spl[i]
                if i<spl_size-1 and spl[i].isnumeric() and spl[i]!='-1':
                    tup = [spl[i], spl[i+1]]
                    res.append(tup)
                    
            if(len(global_res)):
                global_res[-1][1]+=prev
            if(res):
                for l in global_res:
                    line_no = int(l[0])
                    line = l[1]
                    if(line_no!=-1):
                        
                            if not received[int(line_no)]:
                                data_received.append((l[0]+'\n'+l[1]+'\n').encode())
                                # print((l[0]+'\n'+l[1]+'\n').encode())
                                # print()
                                list_of_lines.append((line_no,line))
                                total_received+=1
                                # print("received from slave1 ",total_received)
                                # print("recieved ",total_received)
                                shared_dict[line_no]=line
                                received[line_no]=1

                global_res.clear()

            for tup in res:
                global_res.append(tup)

    
    client_socket.close()

def receive_line_from_slave2():
    global shared_dict,list_of_lines,total_received,received,data_received
    print("listening on thread2")
    host = my_ip
    port = 13002
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.bind((host, port))
    client_socket.listen()
    conn, address = client_socket.accept()
    # print("RECEIVE Connection from: " + str(address))
    while(True):
        if(total_received>=1000):
            break
        received_message = b''
        global_res = []
        while True:  # Continue receiving until a newline is received
            if(total_received>=1000):break
            prev=""
            data_chunk = conn.recv(4096)
            spl = data_chunk.decode().split('\n')
            res = []
            spl_size = len(spl)
            for i in range(spl_size):
                if (i==0 and spl[i].isnumeric() == False):
                    prev = spl[i]
                if i<spl_size-1 and spl[i].isnumeric() and spl[i]!='-1':
                    tup = [spl[i], spl[i+1]]
                    res.append(tup)
                    
            if(len(global_res)):
                global_res[-1][1]+=prev
            if(res):
                for l in global_res:
                    line_no = int(l[0])
                    line = l[1]
                    if(line_no!=-1):
                        if not received[int(line_no)]:
                            data_received.append((l[0]+'\n'+l[1]+'\n').encode())
                            # print((l[0]+'\n'+l[1]+'\n').encode())
                            # print()
                            list_of_lines.append((line_no,line))
                            total_received+=1
                            # print("received from slave1 ",total_received)
                            # print("recieved ",total_received)
                            shared_dict[line_no]=line
                            received[line_no]=1

                global_res.clear()

            for tup in res:
                global_res.append(tup)

    
    client_socket.close()

def receive_line_from_slave3():
    global shared_dict,list_of_lines,total_received,received,data_received
    print("listening on thread3")
    host = my_ip
    port = 14002
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.bind((host, port))
    client_socket.listen()
    conn, address = client_socket.accept()
    # print("RECEIVE Connection from: " + str(address))
    while(True):
        if(total_received>=1000):
            break
        received_message = b''
        global_res = []
        while True:  # Continue receiving until a newline is received
            if(total_received>=1000):break
            prev=""
            data_chunk = conn.recv(4096)
            spl = data_chunk.decode().split('\n')
            res = []
            spl_size = len(spl)
            for i in range(spl_size):
                if (i==0 and spl[i].isnumeric() == False):
                    prev = spl[i]
                if i<spl_size-1 and spl[i].isnumeric() and spl[i]!='-1':
                    tup = [spl[i], spl[i+1]]
                    res.append(tup)
                    
            if(len(global_res)):
                global_res[-1][1]+=prev
            if(res):
                for l in global_res:
                    line_no = int(l[0])
                    line = l[1]
                    if(line_no!=-1):
                        if not received[int(line_no)]:
                            data_received.append((l[0]+'\n'+l[1]+'\n').encode())
                            # print((l[0]+'\n'+l[1]+'\n').encode())
                            # print()
                            list_of_lines.append((line_no,line))
                            total_received+=1
                            # print("received from slave1 ",total_received)
                            # print("recieved ",total_received)
                            shared_dict[line_no]=line
                            received[line_no]=1

                global_res.clear()

            for tup in res:
                global_res.append(tup)

    
    client_socket.close()

def send_lines_to_slave1():
    global data_received,index1,slave1_ip
    host = my_ip
    port = 12007
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.bind((host,port))
    client_socket.listen()
    conn, address = client_socket.accept()
    while(True):
        if(index1>=1000):
            break
        # with line_lock:
        if(index1<len(data_received)):
            line_details = data_received[index1]
            conn.send(line_details)
            # print(line_details,'\n')
            print("sending to slave1 ",index1)
            # print(line_details)
            try:
                received_message= conn.recv(1024).decode()
            except OSError as e:
                break
            if (received_message == "RECEIVED"):
                index1+=1
    client_socket.close()

def send_lines_to_slave2():
    global data_received,index2
    host = my_ip
    port = 13007
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.bind((host,port))
    client_socket.listen()
    conn, address = client_socket.accept()
    while(True):
        if(index2>=1000):
            break
        # with line_lock:
        if(index2<len(data_received)):
            line_details = data_received[index2]
            conn.send(line_details)
            # print(line_details,'\n')
            # print("sending to slave2 ",index2)
            # print(line_details)
            try:
                received_message= conn.recv(1024).decode()
            except OSError as e:
                break
            if (received_message == "RECEIVED"):
                index2+=1
    client_socket.close()

def send_lines_to_slave3():
    global data_received,index3
    host = my_ip
    port = 14007
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.bind((host,port))
    client_socket.listen()
    conn, address = client_socket.accept()
    while(True):
        if(index3>=1000):
            break
        # with line_lock:
        if(index3<len(data_received)):
            line_details = data_received[index3]
            conn.send(line_details)
            # print(line_details,'\n')
            print("sending to slave3 ",index3)
            # print(line_details)
            try:
                received_message= conn.recv(1024).decode()
            except OSError as e:
                break
            if (received_message == "RECEIVED"):
                index1+=1
    client_socket.close()



def main():
    time1=time.time()
    thread1 = threading.Thread(target = receive_lines_from_server)    
    thread1.start() 
    thread2 = threading.Thread(target = receive_line_from_slave1) 
    thread2.start()
    thread3 = threading.Thread(target = receive_line_from_slave2) 
    thread3.start()    
    # thread4 = threading.Thread(target = receive_line_from_slave3)
    # thread4.start()
    thread5 = threading.Thread(target = send_lines_to_slave1) 
    thread5.start()
    thread6 = threading.Thread(target = send_lines_to_slave2) 
    thread6.start()    
    # thread7 = threading.Thread(target = send_lines_to_slave3)
    # thread7.start()


    thread1.join()
    thread2.join()  
    thread3.join()  
    # thread4.join() 
    thread5.join()  
    thread6.join()  
    # thread7.join() 

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(server_address)
    message = "SUBMIT\n"
    message += "2021CS10891@bratva\n"
    message += "1000\n"
    for i in range(1000):
        message+=str(list_of_lines[i][0])
        message+='\n'
        message+=(list_of_lines[i][1])
        message+='\n'
    client_socket.send(message.encode())
    received_message = client_socket.recv(1024)
    print(received_message)
    time2=time.time()
    print("time taken : " ,time2-time1)
    client_socket.close()
    

if __name__ == '__main__':
    main()
