import socket
import threading

server_address = ("vayu.iitd.ac.in",9801)
master_address = ("10.184.21.86",13005)

shared_lines = []
line_lock = threading.Lock()

def receive_lines_from_server():
    global shared_lines
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
            shared_lines.append((line_no,line))
        # print(f"Received from Server: {data}")

def send_lines_to_master():
    global shared_lines
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(master_address)

    while(True):
        with line_lock:
            if shared_lines:
                line_details = shared_lines.pop(0)
                message = str(line_details[0])
                message+="\n"
                message+= line_details[1]
                message+="\n"
                client_socket.send(message.encode())


def main():
    receive_thread = threading.Thread(target = receive_lines_from_server)
    receive_thread.start()

    send_thread = threading.Thread(target = send_lines_to_master)
    send_thread.start()

    receive_thread.join()
    send_thread.join()

    print("Client program finished")

if __name__ == '__main__':
    main()