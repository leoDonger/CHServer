from ast import main
from email import message
from platform import node
from flask import Flask, request, jsonify, Response
from threading import Thread
import json
import time
import logging
import sys
import requests
import hashlib
import bisect
from multiprocessing import Process

# logging.basicConfig(filename='myServer.log',
#                     level=logging.DEBUG,
#                     format='%(message)s')

# logger = logging.getLogger(__name__)
# global app
# app = Flask(__name__)

# global initial_server_count
# initial_server_count = 4
class HashRing:
    def __init__(self, nodes=None, vnodes=100):
        self.vnodes = vnodes
        self.ring = dict()
        self._sorted_keys = []
        
        self.nodes = nodes or []
        for node in self.nodes:
            self.add_node(node)
    
    def _hash(self, key):
        hashValue = hashlib.md5()
        # k = 128
        hashValue.update(key.encode('utf-8'))
        return int(hashValue.hexdigest(), 16)
    
    def add_node(self, node):
        for i in range(self.vnodes):
            key = self._hash(f"{node}:{i}")
            self.ring[key] = node
            bisect.insort(self._sorted_keys, key)
    
    def remove_node(self, node):
        for i in range(self.vnodes):
            key = self._hash(f"{node}_{i}")
            self.ring.pop(key)
            index = bisect.bisect_left(self._sorted_keys, key)
            del self._sorted_keys[index]
    
    def get_node(self, key):
        if not self.ring:
            return None
        hash_val = self._hash(key)
        index = bisect.bisect(self._sorted_keys, hash_val)
        index = index if index < len(self.ring) else 0
        return self.ring[self._sorted_keys[index]]


class MyKVStore:
    def __init__(self, serverName, storageName, port):
        self.app = Flask(serverName)
        self.logger = logging.getLogger(serverName)
        self.serverName = serverName
        self.storage = storageName
        self.port = port
        self.server_kv_store = {}
        
        self.read_data_from_storage()
        logging.basicConfig(filename=f'{serverName}.log',
                    level=logging.DEBUG,
                    format='%(message)s')
        
    def read_data_from_storage(self):
        try:
            with open(self.storage, 'r') as file:
                data = json.load(file)
                print(f"reading kv_store from {self.storage}")
                self.server_kv_store.update(data)
        except Exception as e:
            print(f"An error occurred while reading the file: {e}")
            print(f"Creating loacl kv_store {self.storage}")
        
    def save_data_to_file(self):
        while True:
            try:
                print(f"{self.serverName} is running... Number of keys in store: {len(self.server_kv_store)}")
                with open(self.storage, 'w') as file:
                    json.dump(self.server_kv_store, file)
                    print(f"Data saved to {self.storage}")
                time.sleep(10)  # Adding sleep to avoid high CPU utilization
            except Exception as e:
                print(f"An error occurred: {e}")
        
    def migrate_data_from_another_storage(self, other):
        print(f"Number of keys in {self.serverName} store before migration: {len(self.server_kv_store)}")
        print(f"Number of keys in {other.serverName} store before migration: {len(other.server_kv_store)}")
        
        with open(self.storage, 'w') as file:
            json.dump(self.server_kv_store, file)
            print(f"Data saved to {self.storage}")
            
        other_data = other.server_kv_store
        for key, value in other_data.items():
            self.server_kv_store[key] = value
                    
        print(f"Number of keys in {self.serverName} store before migration: {len(self.server_kv_store)}")
        print(f"Number of keys in {other.serverName} store before migration: {len(other.server_kv_store)}")
        
        with open(self.storage, 'w') as file:
            json.dump(self.server_kv_store, file)
            print(f"Data saved to {self.storage}")
    

    
    def routes(self):
        @self.app.route('/put', methods=['PUT'])
        def put_value():
            key = request.args.get('key')
            value = request.args.get('value')
            self.server_kv_store[key] = value
            return jsonify({'message': 'Value stored successfully'}), 200

        @self.app.route('/get', methods=['GET'])
        def get_value():
            key = request.args.get('key')
            value = self.server_kv_store.get(key)
            if value is not None:
                return jsonify({'value': value}), 200
            else:
                return jsonify({'error': 'Key not found'}), 404
            
        @self.app.route('/del', methods=['DEL'])
        def del_value():
            key = request.args.get('key')
            if key in self.server_kv_store:
                del self.server_kv_store[key]
                return jsonify({'message': 'Key deleted successfully'}), 200
            else:
                return jsonify({'error': 'Key not found'}), 404
        
        @self.app.route('/shutdown', methods=['POST'])
        def shutdown():
            shutdown_func = request.environ.get('werkzeug.server.shutdown')
            if shutdown_func is None:
                raise RuntimeError('Not running with the Werkzeug Server')
            shutdown_func()
            return 'Server shutting down...'
        
        self.app.add_url_rule('/put', view_func=put_value, methods=['PUT'])
        self.app.add_url_rule('/get', view_func=get_value, methods=['GET'])
        self.app.add_url_rule('/del', view_func=del_value, methods=['DELETE'])
        self.app.add_url_rule('/shutdown', view_func=shutdown, methods=['POST'])
    
    def run_server(self):
        Thread(target=self.save_data_to_file, daemon=True).start()
        self.app.run(threaded=True, host='127.0.0.1', port=self.port)
   
  
class MyDistributor: 
    def __init__(self, ring, server_tracker, server_processes):
        self.app = Flask(__name__)
        self.HashRing = ring
        self.added_servers = []
        self.server_tracker = server_tracker
        self.server_processes = server_processes

    def routes(self):
        @self.app.route('/put', methods=['PUT'])
        def put_value():
            key = request.args.get('key')
            value = request.args.get('value')
            print("recevied a put request")
            node = self.HashRing.get_node(key)
            response = requests.put(f"http://127.0.0.1:{node}" + "/put", params={"key": key, "value": value})
            # return response.json(), response.status_code
            try:
                # Attempt to get JSON response if possible
                response_data = response.json()
            except ValueError:
                # If response is not in JSON format, return the raw response text or a custom message
                response_data = {'error': 'Invalid JSON response', 'response_text': response.text}
            return jsonify(response_data), response.status_code
            
        @self.app.route('/get', methods=['GET'])
        def get_value():
            key = request.args.get('key')
            print("recevied a get request")
            node = self.HashRing.get_node(key)
            response = requests.get(f"http://127.0.0.1:{node}" + "/get", params={"key": key})
            # return response.json(), response.status_code
            try:
                # Attempt to get JSON response if possible
                response_data = response.json()
            except ValueError:
                # If response is not in JSON format, return the raw response text or a custom message
                response_data = {'error': 'Invalid JSON response', 'response_text': response.text}
            return jsonify(response_data), response.status_code           
            
        @self.app.route('/del', methods=['DEL'])
        def del_value():
            key = request.args.get('key')
            print("recevied a del request")
            
            node = self.HashRing.get_node(key)
            response = requests.request("DEL", f"http://127.0.0.1:{node}" + "/del", params={"key": key})
            # return response.json(), response.status_code
            try:
                # Attempt to get JSON response if possible
                response_data = response.json()
            except ValueError:
                # If response is not in JSON format, return the raw response text or a custom message
                response_data = {'error': 'Invalid JSON response', 'response_text': response.text}
            return jsonify(response_data), response.status_code
                  
        @self.app.route('/add_server', methods=['POST'])
        def add_server(self):
            new_server_node = f"500{self.server_tracker}"
            self.server_tracker += 1
            self.HashRing.add_node(new_server_node)
            new_port = Process(target=start_server, args=(int(new_server_node),))
            add_server.append(new_port)
            new_port.start()
            return jsonify({'message': "new server added to port"}), 200

        @self.app.route('/remove_server', methods=['POST'])
        def remove_server(self):
            port = request.args.get('port')
            server_to_remove = f"500{port}"
            if (server_to_remove not in self.HashRing.ring):
                return jsonify({'message': f"server at port{port} doesn't exist"}), 400
            
            self.HashRing.remove_node(server_to_remove)
            removed_server = 
            self.HashRing.remove_node(server_to_remove)
            
            try:
                response = requests.post(f"http://127.0.0.1:{server_to_remove}/shutdown")
                if response.status_code == 200:
                    print(f"Server on port {server_to_remove} is shutting down.")
            except requests.exceptions.RequestException as e:
                print(f"Error shutting down server on port {server_to_remove}: {e}")
                
            return jsonify({'message': f"server at port{port} has been removed"}), 200
                        
        self.app.add_url_rule('/put', view_func=put_value, methods=['PUT'])
        self.app.add_url_rule('/get', view_func=get_value, methods=['GET'])
        self.app.add_url_rule('/del', view_func=del_value, methods=['DEL'])
        self.app.add_url_rule('/add_server', view_func=add_server, methods=['POST'])
        self.app.add_url_rule('/remove_server', view_func=remove_server, methods=['POST'])
        
   
    def run_server(self):
        print("The distributor is running")
        self.routes()
        self.app.run(threaded=True, host='127.0.0.1', port=5000)
        for server in self.added_servers:
            server.join()
     
nodes = [] 
port_number_tracker = 1
number_of_servers = 5
while port_number_tracker <= number_of_servers:
    nodes.append(f"500{port_number_tracker}")    
    port_number_tracker += 1    
ring = HashRing(nodes, vnodes=100)
myDistributor = MyDistributor(ring, port_number_tracker)


def start_server(port):
    server_name = f'MyKVServer{port}'
    storage_name = f'storage{port}.json'
    kv_store = MyKVStore(server_name, storage_name, port)
    kv_store.routes()
    kv_store.run_server()
        

if __name__ == '__main__':
    servers = [
        Process(target=start_server, args=(int(node),)) for node in nodes
    ]
    servers.append(Process(target=myDistributor.run_server))
    
    # Start all servers
    for server in servers:
        server.start()
        
    # Join all servers to the main thread to prevent the script from finishing prematurely
    for server in servers:
        server.join()
