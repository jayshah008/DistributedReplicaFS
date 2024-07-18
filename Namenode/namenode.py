import socket
import threading
import os
import json
import logging
import time


logging.basicConfig(filename='namenode.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class NameNode:
    def __init__(self, host, port, min_datanodes=3):
        self.host = host
        self.port = port
        self.data_nodes = {}
        self.block_assignments = {}
        self.min_datanodes = min_datanodes
        self.connected_datanodes = 0
        self.connection_condition = threading.Condition()
        self.metadata_file = "metadata.json"
        self.load_metadata()
        self.uploaded_files = set()

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.host, self.port))
            server_socket.listen()

            print(f"NameNode listening on {self.host}:{self.port}")

            while True:
                client_socket, client_address = server_socket.accept()
                threading.Thread(target=self.handle_data_node, args=(client_socket,)).start()

    def handle_data_node(self, client_socket):
        with client_socket:
            data = client_socket.recv(1024).decode("utf-8")
            parts = data.split()

            if len(parts) >= 0:
                command = parts[0]

                if command == "REGISTER":
                    try:
                        host = parts[1]
                        port = int(parts[2])
                        node_id = f"{host}:{port}"

                        with self.connection_condition:
                            self.data_nodes[node_id] = (host, port)
                            self.connected_datanodes += 1

                            print(f"DataNode {node_id} registered from {host}:{port}")
                            client_socket.sendall(b"REGISTRATION_SUCCESS")

                            if self.connected_datanodes >= self.min_datanodes:
                                print(f"Minimum required DataNodes ({self.min_datanodes}) connected. Ready for upload.")
                                self.connection_condition.notify_all()

                                while self.connected_datanodes < self.min_datanodes:
                                    self.connection_condition.wait()

                                threading.Thread(target=self.handle_heartbeat, args=(client_socket,)).start()

                                while True:
                                    user_choice = input("Choose an option:\n1. Upload\n2. Download\n3. List the files\n4. Exit\n")
                                    if user_choice == "1":
                                        file_path = input("Enter the file path to upload: ")
                                        file_name = os.path.basename(file_path)
                                        self.handle_upload_request(file_path, file_name)
                                    elif user_choice == "2":
                                        file_name = input("Enter the file name to download: ")
                                        self.handle_download_request(file_name)
                                    elif user_choice == "3":
                                        self.list_files()
                                    elif user_choice == "4":
                                        print("Exiting...")
                                        self.clear_metadata()
                                        return
                                    else:
                                        print("Invalid choice.")

                    except (IndexError, ValueError):
                        print("Error: Malformed registration message.")

                elif command == "HEARTBEAT_ACK":
                    logging.info(f"Received heartbeat acknowledgment from datanode {parts[1]}")

                else:
                    print("Error: Unknown command.")
                    print('It is :')
                    print(parts)

            else:
                print("Error: Malformed message.")

    def handle_upload_request(self, file_path, file_name):

        if not os.path.isfile(file_path):
            print(f"Error: File '{file_path}' does not exist in the local directory. Choose a valid file.")
            return
        
        if file_name in self.uploaded_files:
            print(f"Error: File '{file_name}' already exists. Choose a different file name.")
            return

        with open(file_path, "r") as file:
            file_data = file.read()

        print(f"File {file_name} uploaded successfully.")

        self.uploaded_files.add(file_name)

        block_size = 64
        block_ids = [f"{file_name}_block_{i}" for i in range(1, len(file_data) // block_size + 2)]

        for i, block_id in enumerate(block_ids):
            assigned_datanode = self.get_next_datanode()
            self.block_assignments[block_id] = assigned_datanode

            start_index = i * block_size
            end_index = min((i + 1) * block_size, len(file_data))

            block_data = file_data[start_index:end_index]

            assigned_datanode_address = assigned_datanode.split(':')[0]
            assigned_datanode_port = int(assigned_datanode.split(':')[1])
            print(f"Attempting to connect to DataNode at {assigned_datanode_address}:{assigned_datanode_port}")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as datanode_socket:
                try:
                    datanode_socket.connect((assigned_datanode_address, assigned_datanode_port))
                    print("Connection successful.")
                    print("The block data is:", f'---{block_data}---')
                    datanode_socket.sendall(f"UPLOAD_BLOCK?>?{block_id}?>?{block_data}".encode("utf-8"))
                    self.replicate_block(block_id, block_data)
                except Exception as e:
                    print(f"Error connecting to DataNode: {e}")

        self.save_metadata()

    def replicate_block(self, block_id, block_data):
        replication_factor = 1
        for _ in range(replication_factor):
            replica_datanode = self.get_next_datanode()
            replica_datanode_address = replica_datanode.split(':')[0]
            replica_datanode_port = int(replica_datanode.split(':')[1])
            print(f"Replicating block {block_id} to DataNode at {replica_datanode_address}:{replica_datanode_port}")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as replica_socket:
                try:
                    replica_socket.connect((replica_datanode_address, replica_datanode_port))
                    print("Replication successful.")
                    block_id = block_id + "_Replica"
                    replica_socket.sendall(f"UPLOAD_BLOCK?>?{block_id}?>?{block_data}".encode("utf-8"))
                except Exception as e:
                    print(f"Error replicating block to DataNode: {e}")

        self.save_metadata()

    def handle_download_request(self, file_name):
        if file_name not in self.uploaded_files:
            print(f"Error: File '{file_name}' does not exist. Choose a different file name.")
            print("The files are:")
            self.list_files()
            return

        block_ids = [key for key in self.block_assignments if file_name in key]

        file_content = b""
        for block_id in block_ids:
            assigned_datanode = self.block_assignments[block_id]
            assigned_datanode_address = assigned_datanode.split(':')[0]
            assigned_datanode_port = int(assigned_datanode.split(':')[1])

            print(f"Attempting to connect to DataNode at {assigned_datanode_address}:{assigned_datanode_port}")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as datanode_socket:
                try:
                    datanode_socket.connect((assigned_datanode_address, assigned_datanode_port))
                    print("Connection successful.")
                    datanode_socket.sendall(f"DOWNLOAD_BLOCK?>?{block_id}".encode("utf-8"))
                    block_data = datanode_socket.recv(1024)
                    file_content += block_data
                except Exception as e:
                    print(f"Error connecting to DataNode: {e}")

        downloaded_file_path = f"downloaded_{file_name}"
        with open(downloaded_file_path, "wb") as downloaded_file:
            downloaded_file.write(file_content)

        print(f"File {file_name} downloaded successfully. Saved as {downloaded_file_path}")

    def get_next_datanode(self):
        datanode_ids = list(self.data_nodes.keys())
        next_datanode = datanode_ids[len(self.block_assignments) % len(datanode_ids)]
        return next_datanode

    def save_metadata(self):
        with open(self.metadata_file, "w") as metadata_file:
            json.dump(self.block_assignments, metadata_file)

    def load_metadata(self):
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, "r") as metadata_file:
                self.block_assignments = json.load(metadata_file)

    def list_files(self):
        print("root")
        for file_name in self.uploaded_files:
            print(f"|\n|\n- - - - -{file_name}")

    def handle_heartbeat(self, client_socket):
        print('Sending heartbeat')
        client_socket.sendall(b"HEARTBEAT_ACK")

    def clear_metadata(self):
        self.block_assignments = {}
        self.save_metadata()
        logging.shutdown()


if __name__ == "__main__":
    print("Enter the number of namenodes to be connected:")
    namenode_count = int(input())
    nn = NameNode("127.0.0.1", 5001,namenode_count)
    nn.start()


