from csv import writer
from math import fabs
import socket
import time
import threading
from urllib import response
import urllib.parse
import requests
import json
import random
import csv

BASE_URL = "http://127.0.0.1:5000"

HOST, PORT = '127.0.0.1', 5000
NUM_REQUESTS = 1000  # Number of requests to send per client
NUM_CLIENTS = 10    # Number of concurrent clients
READ_PERCENTAGE = 0.95  # Percent of Read
ALL_METHODS = False  # Flag for testing all three methods from each client
TEST_DISTRUBUTION = True   # Flag for testing two methods (read and write) from each client

TEST_ADD_OR_REMOVE = False # Flag for testing adding/removing server behavior with a benefit of Consistent hashing
# Please change NUM_REQUESTS and NUM_CLIENTS to 1 to pervent chaostic behavior


def put_value(key, value):
    """
    Client side put request function
    """
    try:
        response = requests.put(BASE_URL + "/put", params={"key": key, "value": value})
        if response.status_code == 200:
            print("PUT successful:", response.json())
        else:
            print("Failed to PUT:", response.text)
    except Exception as e:
        print("Error occurred:", e)


def get_value(key):
    """
    Client side get request function
    """
    try:
        response = requests.get(BASE_URL + "/get", params={"key": key})
        if response.status_code == 200:
            print("GET successful:", response.json())
        elif response.status_code == 404:
            print("Key not found")
        else:
            print("Failed to GET:", response.text)
    except Exception as e:
        print("Error occurred:", e)


def del_value(key):
    """
    Client side del method request function
    """
    try:
        response = requests.request("DEL", BASE_URL + "/del", params={"key": key})
        if response.status_code == 200:
            print("DELETE successful:", response.json())
        elif response.status_code == 404:
            print("Key not found")
        else:
            print("Failed to DELETE:", response.text)
    except Exception as e:
        print("Error occurred:", e)
        
        
def add_server():
    """
    Client side function to send a add server request to the main distrubutor server
    """
    try:
        response = requests.post(BASE_URL + "/add_server")
        if response.status_code == 200:
            print("Adding server successful:", response.json())
        else:
            print("Adding server failed", response.text)
    except Exception as e:
        print("Error occurred:", e)


def remove_server(port):
    """
    Client side function to send a add server request to the main distrubutor server
    """
    try:
        response = requests.post(BASE_URL + "/remove_server", params={"port": port})
        if response.status_code == 200:
            print("Adding server successful:", response.json())
        else:
            print("Removing server failed", response.text)
    except Exception as e:
        print("Error occurred:", e)
        

# compute reasonable csv file name containing Latency/Throughput pairs  
if (ALL_METHODS):
    filename = f"Latency_over_Throughput_with_all_three_methods_{NUM_CLIENTS}_clients.csv" 
elif TEST_DISTRUBUTION:
    filename = "Latency_over_Throughput_with_read_and_write.csv" 
else:
    filename = f"Latency_over_Throughput_with_{READ_PERCENTAGE}_read.csv" 


def should_read():
    """
    Function for percentage of read and write 
    """
    return random.random() < READ_PERCENTAGE

# example program from professor Annwar to generate Latency/Throughput
def client_thread(client_id):
    start_time = time.time()
    
    if (ALL_METHODS):
        number_of_methods = 3
    elif (TEST_DISTRUBUTION):
        number_of_methods = 2
    else:
        number_of_methods = 1
    
    for i in range(NUM_REQUESTS):
        key = f"key-{client_id}-{i}"
        value = f"value-{client_id}-{i}"
        port = "5001"
        
        if (ALL_METHODS):
            put_value(key, value)
            get_value(key)
            del_value(key)
        elif (TEST_DISTRUBUTION):
            put_value(key, value)
            get_value(key)
        elif (TEST_ADD_OR_REMOVE):
            add_server()
            remove_server(port)
        else:
            if should_read():
                get_value(key)
            else:
                put_value(key, value)
    
    # End time
    end_time = time.time()
    elapsed_time = end_time - start_time
    throughput = NUM_REQUESTS * number_of_methods / elapsed_time  
    latency = elapsed_time / (NUM_REQUESTS * number_of_methods)

    print(f"Client-{client_id} | Throughput: {throughput:.2f} req/s | Average Latency: {latency:.6f} seconds")
    
    # list of Latency/Throughput pair
    LT = [f"{latency:.6f}", f"{throughput:.2f}"]
    with open(filename, 'a') as file:
        writer = csv.writer(file)
        writer.writerow(LT)

if __name__ == "__main__":
    clients = []

    for i in range(NUM_CLIENTS):
        t = threading.Thread(target=client_thread, args=(i,))
        clients.append(t)
        t.start()

    for t in clients:
        t.join()

    print("Testing completed.")