# Basic Info
- Language: Golang

# Server Configure
There must be 3 configure files under directory `server/`.
- `server.ini`: General configure for all servers, including log path, port, etc.
- `cluster.json`: A general json file for the cluster information, including a list of VMs (hosts) and their hostname/port/machine ID.
- `machine_info.ini`: A machine based configure file, including machine ID and hostname. Note that this file is different on 10 machines, so it should be deployed manually and only one time.

# How to Compile and Run
## Server
Please run the server with all configure files mentioned above. To make it simple, we run it under `server/`.
```
$ cd server/
$ go build
$ ./server
```

## Client
- MP1: Grep log file (Note that there should be a colon before the port argument)
```
$ cd client/
$ go build
$ ./client [hostname/IP] :[port] [regex]
```

## Unit Test
- MP1: Distributed grep file
```
$ cd common/
$ go test
```
