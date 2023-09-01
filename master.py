import socket
import threading

server_address = ("vayu.iitd.ac.in", 9801)


received = [0 for i in range(1001)]
list_of_lines = []
shared_dict = {}
total_received = 0
line_lock = threading.Lock()
my_ip = "10.194.20.56"
def receive_lines_from_server():
    global shared_dict,list_of_lines,total_received,received
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(server_address)

    while(True):
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
        with line_lock:
            if not received[line_no]:
                list_of_lines.append((line_no,line))
                total_received+=1
                shared_dict[line_no]=line
                received[line_no]=1

def receive_line_from_slave1():
    global shared_dict,list_of_lines,total_received,received
    print("listening on thread1")
    host = my_ip
    port = 12001
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.bind((host, port))
    client_socket.listen()
    conn, address = client_socket.accept()
    print("Connection from: " + str(address))
    while(True):
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
        # Print the received data
        split_data = data.split('\n')
        line_no = int(split_data[0])
        line = split_data[1]        
        with line_lock:
            if not received[line_no]:
                list_of_lines.append((line_no,line))
                total_received+=1
                shared_dict[line_no]=line
                received[line_no]=1

def receive_line_from_slave2():
    global shared_dict,list_of_lines,total_received,received
    print("listening on thread2")
    host = my_ip
    port = 13001
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.bind((host, port))
    client_socket.listen()
    conn, address = client_socket.accept()
    print("Connection from: " + str(address))
    while(True):
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
        # Print the received data
        split_data = data.split('\n')
        line_no = int(split_data[0])
        line = split_data[1]        
        with line_lock:
            if not received[line_no]:
                list_of_lines.append((line_no,line))
                total_received+=1
                shared_dict[line_no]=line
                received[line_no]=1

def receive_line_from_slave3():
    global shared_dict,list_of_lines,total_received,received
    print("listening on thread3")
    host = my_ip
    port = 14001
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.bind((host, port))
    client_socket.listen()
    conn, address = client_socket.accept()
    print("Connection from: " + str(address))
    while(True):
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
        # Print the received data
        split_data = data.split('\n')
        line_no = int(split_data[0])
        line = split_data[1]        
        with line_lock:
            if not received[line_no]:
                list_of_lines.append((line_no,line))
                total_received+=1
                shared_dict[line_no]=line
                received[line_no]=1


def main():
    thread1 = threading.Thread(target = receive_lines_from_server)    
    thread1.start() 
    thread2 = threading.Thread(target = receive_line_from_slave1) 
    thread2.start()
    thread3 = threading.Thread(target = receive_line_from_slave2) 
    thread3.start()    
    thread4 = threading.Thread(target = receive_line_from_slave3)
    thread4.start()

    thread1.join()
    thread2.join()  
    thread3.join()  
    thread4.join()  

if __name__ == '__main__':
    main()