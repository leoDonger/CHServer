# My Key-Value Store With Consistent Hashing Documentation

This documentation provides an overview and setup guide for a key-value store system based on Flask. The system is designed to create a introduction level, scalable, key-value store distributed over multiple nodes (default 5) using consistent hashing via a hash ring.

## Overview

The system comprises two primary components: individual key-value store servers (`MyKVStore`) and a distributor (`MyDistributor`) which manages the requests and distributes them to the appropriate server based on consistent hashing.

### `HashRing`

The `HashRing` class is responsible for implementing consistent hashing. Each node on the ring can host multiple virtual nodes (vnodes), increasing the distribution's evenness and fault tolerance.

### `MyKVStore`

`MyKVStore` represents an individual key-value store server. It is responsible for handling storage operations such as `GET`, `PUT`, and `DELETE`. It also manages data persistence by periodically saving data to a JSON file.

### `MyDistributor`

`MyDistributor` acts as a load balancer and is the entry point for client requests. It uses the `HashRing` to determine which `MyKVStore` instance should handle a given request and forwards the request accordingly.

### API Endpoints

The key-value store supports the following RESTful endpoints:

- **PUT /put**: Stores a value with the specified key.
- **GET /get**: Retrieves the value associated with the specified key.
- **DELETE /del**: Deletes the specified key and its value.
- **POST /add_server**: Adds a new server to the hash ring and starts it.
- **POST /remove_server**: Removes a server from the hash ring and shuts it down.

### Example Requests

```http
PUT http://127.0.0.1:5000/put?key=myKey&value=myValue
```

```http
GET http://127.0.0.1:5000/get?key=myKey
```

```http
DELETE http://127.0.0.1:5000/del?key=myKey
```

## Shutdown

To safely shut down a server, use the `http://127.0.0.1:5000/shutdown?port=portnumber` endpoint, which will gracefully stop the server after completing any ongoing requests.

## Storage

Each `MyKVStore` server stores its data in a separate JSON file. The files are periodically updated to reflect the current state of the key-value store.

## Scalability

The system can handle the dynamic addition and removal of servers. The `HashRing` automatically redistribates keys when servers are added or removed to maintain an even load distribution.


To add the testing script to a `README.md` file, you'll want to provide some context for why the script is included, how to use it, and what its purpose is. Below is an example of how you can document this script in your `README.md` file:

---

## Testing Script

myClient.py is created and used for testing the server's response for various operations like GET, PUT, DEL,handling server additions and removals, and measuring latency and throughput.

### Script Description

- `BASE_URL` is the address where your server is running.
- `NUM_REQUESTS` specifies how many requests each client will send.
- `NUM_CLIENTS` specifies how many concurrent clients will be sending requests.
- `READ_PERCENTAGE` sets the proportion of read operations to total operations. It is utilized after all other testing control flags are off. 
- Flags like `ALL_METHODS` and `TEST_DISTRIBUTION` control the behavior of the test.
- `TEST_ADD_OR_REMOVE` allows testing server add/remove functionality in consistent hashing.

### Output

The script will print out the status of each request and will create a CSV file with latency and throughput data for analysis.