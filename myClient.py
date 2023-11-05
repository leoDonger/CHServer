from csv import writer
import socket
import time
import threading
import urllib.parse
import requests
import json
import random
import csv

BASE_URL = "http://127.0.0.1:5000"

HOST, PORT = '127.0.0.1', 5000
NUM_REQUESTS = 1000  # Number of requests to send per client
NUM_CLIENTS = 10     # Number of concurrent clients
READ_PERCENTAGE = 0.95
ALL_METHODS = False
TEST_DISTRUBUTION = True

def put_value(key, value):
    try:
        response = requests.put(BASE_URL + "/put", params={"key": key, "value": value})
        if response.status_code == 200:
            print("PUT successful:", response.json())
        else:
            print("Failed to PUT:", response.text)
    except Exception as e:
        print("Error occurred:", e)

def get_value(key):
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

LT_list = []
filename = f"Latency_over_Throughput_with_{READ_PERCENTAGE}_read" if not ALL_METHODS else "Latency_over_Throughput_with_all_three_methods"

def should_read():
    return random.random() < READ_PERCENTAGE

def client_thread(client_id):
    start_time = time.time()
    # read_count = write_count = 0
    if (ALL_METHODS):
        number_of_methods = 3
    elif (TEST_DISTRUBUTION):
        number_of_methods = 2
    else:
        number_of_methods = 1
    
    for i in range(NUM_REQUESTS):
        key = f"key-{client_id}-{i}"
        value = f"value-{client_id}-{i}"
        
        if (ALL_METHODS):
            put_value(key, value)
            get_value(key)
            del_value(key)
        elif (TEST_DISTRUBUTION):
            put_value(key, value)
            get_value(key)
        else: 
            if should_read():
                get_value(key)
                # read_count += 1
            else:
                put_value(key, value)
                # write_count += 1
    
    # End time
    end_time = time.time()
    elapsed_time = end_time - start_time
    throughput = NUM_REQUESTS * number_of_methods / elapsed_time  
    latency = elapsed_time / (NUM_REQUESTS * number_of_methods)

    print(f"Client-{client_id} | Throughput: {throughput:.2f} req/s | Average Latency: {latency:.6f} seconds")
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