import socket
import threading
import os
import time

class DataNode:
    def __init__(self, host, port, namenode_host, namenode_port):
        self.host = host
        self.port = port
        self.namenode_host = namenode_host
        self.namenode_port = namenode_port

    def register_with_namenode(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.connect((self.namenode_host, self.namenode_port))

            registration_message = f"REGISTER {self.host} {self.port}"
            client_socket.sendall(registration_message.encode("utf-8"))

            response = client_socket.recv(1024).decode("utf-8")
            print(response)

    def send_heartbeat(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.connect((self.namenode_host, self.namenode_port))
            heartbeat_message = f"HEARTBEAT_ACK {self.host}:{self.port}"
            client_socket.sendall(heartbeat_message.encode("utf-8"))
            response = client_socket.recv(1024).decode("utf-8")
            print(response)  


    def send_heartbeat_periodically(self):
        heartbeat_interval = 10  
        while True:
            time.sleep(heartbeat_interval)
            self.send_heartbeat()

    def handle_upload_block(self, block_id, block_data):
        datanode_folder = "datanodes"
        block_path = os.path.join(datanode_folder, block_id)

        os.makedirs(os.path.dirname(block_path), exist_ok=True)

        with open(block_path, "wb") as block_file:  
            block_file.write(block_data.encode("utf-8"))

    def handle_download_block(self, block_id, client_socket):
        datanode_folder = "datanodes"
        block_path = os.path.join(datanode_folder, block_id)

        try:
            with open(block_path, "rb") as block_file:
                block_data = block_file.read()
                client_socket.sendall(block_data)
        except FileNotFoundError:
            print(f"Block {block_id} not found.")

    def start(self):
        threading.Thread(target=self.send_heartbeat_periodically).start()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.host, self.port))
            server_socket.listen()

            print(f"DataNode listening on {self.host}:{self.port}")

            while True:
                client_socket, client_address = server_socket.accept()
                threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def handle_client(self, client_socket):
        with client_socket:
            data = client_socket.recv(1024).decode("utf-8")
            parts = data.split('?>?')

            if len(parts) >= 2:
                command = parts[0]

                if command == "UPLOAD_BLOCK":
                    try:
                        block_id = parts[1]
                        block_data = parts[2]
                        self.handle_upload_block(block_id, block_data)
                        client_socket.sendall(b"UPLOAD_BLOCK_SUCCESS")
                    except IndexError:
                        print("Error: Malformed upload block message.")
                        client_socket.sendall(b"UPLOAD_BLOCK_ERROR")
                elif command == "DOWNLOAD_BLOCK":
                    try:
                        block_id = parts[1]
                        self.handle_download_block(block_id, client_socket)
                    except IndexError:
                        print("Error: Malformed download block message.")
                        client_socket.sendall(b"DOWNLOAD_BLOCK_ERROR")

                elif command == "HEARTBEAT":
                    print("Heartbeat from namenode recieved")
                    client_socket.sendall(b"HEARTBEAT_ACK")

                else:
                    print("Error: Unknown command.")

if __name__ == "__main__":
    dn = DataNode("127.0.0.1", 6000, "127.0.0.1", 5001)
    dn.register_with_namenode()
    dn.start()
