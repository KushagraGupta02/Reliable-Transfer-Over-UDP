import socket
import threading
import time

server_address = ("10.17.6.5",9801)
master_address = ("10.194.27.67",14001)
master_address2 = ("10.194.27.67",14008)

shared_lines = []
received_dict = {}
received_lines = 0
lines_send = 0
line_lock = threading.Lock()

def receive_lines_from_server():
    global shared_lines,received_lines
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client_socket.connect(server_address)
    except:
        print("server not running")
        return

    while(True):
        if(received_lines>=1000):
            break
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
        # Convert the binary data to a string and decode
        data = received_message
        # with line_lock:
        shared_lines.append(data)
    client_socket.close()
    

def send_lines_to_master():
    global shared_lines,lines_send
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client_socket.connect(master_address)
    except:
        print("master client not running")
        return
    
    while(True):
        if(received_lines>=1000):
            break
        # with line_lock:
        if shared_lines:
            line_details = shared_lines.pop(0)
            # print(line_details)
            # print('\n')
            data = line_details.decode()
            split_data = data.split('\n')
            if(not split_data[0].isdigit()):continue
            line_no = int(split_data[0])
            line = split_data[1]
            if(line_no==-1):continue
            lines_send += 1
            print("sending to master ",lines_send)
            print('\n')
            try:
                client_socket.send(line_details)
            except OSError as e:
                if e.errno == 9 or e.errno == 32:
                    client_socket.close()
                    
    client_socket.close()
    

def get_lines_from_master():
    global received_dict,received_lines
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client_socket.connect(master_address2)
    except:
        print("master client not running")
        return
    
    while(True):
        if(received_lines>=1000):
            break
        received_message = b''
        while True:  # Continue receiving until a newline is received
            data_chunk = client_socket.recv(4096)
            if not data_chunk:  # No more data to receive
                break
            received_message += data_chunk  # Append the received chunk
            # Check if the received data ends with a newline character
            if received_message.endswith(b'\n'):
                break
        # Convert the binary data to a string and decode
        # print(received_message)
        print(received_message)
        # print('\n')
        data = received_message.decode()
        # Print the received data
        split_data = data.split('\n')
        if(not split_data[0].isdigit()):continue
        line_no = int(split_data[0])
        line = split_data[1] 
        if(line_no==-1):continue
        received_dict[line_no]=line   
        # print(line)
        received_lines += 1 
        print("received from master ",received_lines)
        print('\n')
        # print('\n')   
        message = "RECEIVED"
        client_socket.send(message.encode())
        

    client_socket.close()
    




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
            print("mein tut gaya hun ",i)
        message+=m
        message+='\n'
    client_socket.send(message.encode())
    received_message = client_socket.recv(1024)
    print(received_message)
    time2 = time.time()
    print("time taken ",time2-time1)
    client_socket.close()
    
    print("Client program finished")

if __name__ == '__main__':
    main()
