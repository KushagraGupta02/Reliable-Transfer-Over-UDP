import socket
import threading
import time

server_address = ("vayu.iitd.ac.in",9801)
master_address = ("10.184.35.249",12001) #ip address of master client and port for sending
master_address2 = ("10.184.35.249",12002) #ip address of master client and port for receiving

shared_lines = []
received_dict = {}
received_lines = 0
lines_send = 0
line_lock = threading.Lock()
connection_true = 0
master_running = 0

def receive_lines_from_server():
    global shared_lines,received_lines,connection_true
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    MAX_RECONNECT_ATTEMPTS = 5
    reconnect_attempts = 0
    while reconnect_attempts < MAX_RECONNECT_ATTEMPTS:
        try:
            client_socket.connect(server_address)
            break
        except (BrokenPipeError, ConnectionRefusedError, TimeoutError) as e:
            reconnect_attempts += 1
            if reconnect_attempts > MAX_RECONNECT_ATTEMPTS:
                print("Maximum reconnection attempts reached. Exiting.")
                client_socket.close()
                return

    while(True):
        if(received_lines>=1000 or connection_true==2 or master_running>=5):
            client_socket.close()
            return
        message = "SENDLINE\n"
        client_socket.send(message.encode())
        received_message = b''
        while True:  # Continue receiving until a newline is received
            data_chunk = client_socket.recv(4096)
            if not data_chunk:  # No more data to receive
                break
            received_message += data_chunk  # Append the received chunk
            # Check if the received data ends with a newline character
            if received_message.endswith(b'\n'):
                break
        data = received_message
        shared_lines.append(data)
    

def send_lines_to_master():
    global shared_lines,lines_send,connection_true
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    MAX_RECONNECT_ATTEMPTS = 5
    reconnect_attempts = 0
    while reconnect_attempts < MAX_RECONNECT_ATTEMPTS:
        try:
            client_socket.connect(master_address)
            break
        except (BrokenPipeError, ConnectionRefusedError, TimeoutError) as e:
            reconnect_attempts += 1
            if reconnect_attempts >= MAX_RECONNECT_ATTEMPTS:
                print("Maximum reconnection attempts reached. Exiting.")
                connection_true+=1
                client_socket.close()
                return
    
    while(True):
        if(received_lines>=1000 or master_running>=5):
            client_socket.close()
            return
        
        if shared_lines:
            line_details = shared_lines.pop(0)
            data = line_details.decode()
            split_data = data.split('\n')
            if(not split_data[0].isdigit()):continue
            line_no = int(split_data[0])
            line = split_data[1]
            if(line_no==-1):continue
            lines_send += 1
            # print("sending to master ",lines_send)
            try:
                client_socket.send(line_details)
            except OSError as e:
                if e.errno == 9 or e.errno == 32:
                    client_socket.close()

    

def get_lines_from_master():
    global received_dict,received_lines,connection_true,master_running
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    MAX_RECONNECT_ATTEMPTS = 5
    reconnect_attempts = 0
    while reconnect_attempts < MAX_RECONNECT_ATTEMPTS:
        try:
            client_socket.connect(master_address2)
            break
        except (BrokenPipeError, ConnectionRefusedError, TimeoutError) as e:
            reconnect_attempts += 1
            if reconnect_attempts >= MAX_RECONNECT_ATTEMPTS:
                print("Maximum reconnection attempts reached. Exiting.")
                connection_true+=1
                client_socket.close()
                return
    while(True):
        if(received_lines>=1000 or master_running>=5):
            client_socket.close()
            return
        
        received_message = b''
        while True:  # Continue receiving until a newline is received
            data_chunk = client_socket.recv(4096)
            if not data_chunk:  # No more data to receive
                break
            received_message += data_chunk  # Append the received chunk
            # Check if the received data ends with a newline character
            if received_message.endswith(b'\n'):
                break
        if(received_message==b''):master_running+=1
        # print(received_message)
        data = received_message.decode()
        split_data = data.split('\n')
        if(not split_data[0].isdigit()):continue
        line_no = int(split_data[0])
        line = split_data[1] 
        if(line_no==-1):continue
        received_dict[line_no]=line 
        received_lines += 1 
        # print("received from master ",received_lines)
        # print('\n') 
        message = "RECEIVED"
        client_socket.send(message.encode())
        
    

def main():
    time1 = time.time()
    receive_thread = threading.Thread(target = receive_lines_from_server)
    receive_thread.start()

    send_thread = threading.Thread(target = send_lines_to_master)
    send_thread.start()

    get_thread = threading.Thread(target = get_lines_from_master)
    get_thread.start()

    receive_thread.join()
    send_thread.join()
    get_thread.join()
    if(connection_true==2 or master_running>=5):
        print("master not running or stopped running")
        return
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(server_address)
    message = "SUBMIT\n"
    message += "2021CS10891@bratva\n"
    message += "1000\n"
    for i in range(1000):
        message+=str(i)
        message+='\n'
        m = received_dict.get(i)
        if m is None:
            message+="exception"
            message+='\n'
            continue
        message+=m
        message+='\n'
    MAX_RECONNECT_ATTEMPTS = 5
    reconnect_attempts = 0
    while reconnect_attempts < MAX_RECONNECT_ATTEMPTS:
        try:
            client_socket.send(message.encode())
            received_message = client_socket.recv(4096)
            break  # Connection successful, exit the loop
        except (BrokenPipeError, ConnectionRefusedError, TimeoutError) as e:
            reconnect_attempts += 1
            if reconnect_attempts >= MAX_RECONNECT_ATTEMPTS:
                print("Maximum reconnection attempts reached. Exiting.")
                return
        print(f"Reconnecting (Attempt {reconnect_attempts})...")
        client_socket.connect(server_address)
    print(received_message)
    time2 = time.time()
    print("time taken ",time2-time1)
    client_socket.close()
    
    print("Client program finished")

if __name__ == '__main__':
    main()