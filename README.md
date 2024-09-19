# rust-minikeyvalue

minikeyvalue is a distributed key-value store with a simple HTTP interface, written in Rust. This project is a port of the [original Go implementation by George Hotz](https://github.com/geohot/minikeyvalue/tree/master), used in production at comma.ai.

## Features

* Simple HTTP interface for PUT, GET, DELETE operations
* Supports range requests for partial retrieval of values
* Supports HEAD requests for metadata retrieval
* Supports listing of keys with optional limit and start parameters
* Content-MD5 header for data integrity verification
* Reverse proxy with nginx for load balancing and SSL termination
* Optimized for values between 1MB and 1GB, scalable to billions of files and petabytes of data
* Utilizes a simple on-disk format, relying on a filesystem for blob storage and a LevelDB for indexing
* Supports dynamic volume management with rebalance and index reconstruction with rebuild
* Can handle petabytes of data
* We do not support a subset of S3 for now


## API

### API Endpoints

#### PUT /key
Create a new key-value pair. If the key already exists, returns a 403 Forbidden response.

* **Status Code**:
	+ 201: Key-value pair created successfully.
	+ Other: Creation failed, data may not be written.
* **Description**: Creates a new key-value pair. If the key already exists, returns a 403 Forbidden response.
* **Example**: `curl -v -L -X PUT -d bigswag localhost:3000/wehave`

#### PUT /key (File)
Create a new key-value pair with a file.

* **Status Code**:
	+ 201: Key-value pair created successfully.
	+ Other: Creation failed, data may not be written.
* **Description**: Creates a new key-value pair with a file.
* **Example**: `curl -v -L -X PUT -T /path/to/local/file.txt localhost:3000/file.txt`

#### GET /key
Retrieve the value associated with a key.

* **Status Code**: 302 (redirect to nginx volume server)
* **Description**: Redirects to the nginx volume server to retrieve the value associated with the key.
* **Examples**: 
  * GET request: `curl -v -L localhost:3000/wehave`
  * HEAD request: `curl -v -L -I localhost:3000/wehave`

#### GET /key (File)
Retrieve a file associated with a key.

* **Status Code**: 302 (redirect to nginx volume server)
* **Description**: Redirects to the nginx volume server to retrieve the file associated with the key.
* **Example**: `curl -v -L -o /path/to/local/file.txt localhost:3000/file.txt`

#### DELETE /key
Delete a key-value pair.

* **Status Code**:
	+ 204: Key-value pair deleted successfully.
	+ Other: Deletion failed, data may still exist.
* **Description**: Deletes a key-value pair.
* **Example**: `curl -v -L -X DELETE localhost:3000/wehave`

#### UNLINK /key
Mark a key-value pair for deletion (virtual delete).

* **Description**: Marks a key-value pair for deletion.
* **Example**: `curl -v -L -X UNLINK localhost:3000/wehave`

#### GET /prefix?list
List keys starting with a given prefix.

* **Description**: Lists keys starting with the given prefix.
* **Example**: `curl -v -L localhost:3000/we?list`

#### GET /?unlinked
List unlinked keys ripe for deletion.

* **Description**: Lists unlinked keys that are ripe for deletion.
* **Example**: `curl -v -L localhost:3000/?unlinked`

## Performance benchmarks

The code performs equal or better than the original Go implementation.

Our implementatio results first, second the Go implementation!

````
rust-minikeyvalue git:(master) ✗ go run tools/thrasher.go
starting thrasher
20000 write/read/delete in 7.778764375s
thats 2571.10/sec
```

````
minikeyvalue git:(master) ✗ go run tools/thrasher.go
starting thrasher
20000 write/read/delete in 7.651901291s
thats 2613.73/sec
```

As you can see the avg latency, stdv and max are way better than the Go implementation. Around 35% improvement!
````
rust-minikeyvalue git:(master) ✗ wrk -t2 -c100 -d10s http://localhost:3000/key

Running 10s test @ http://localhost:3000/key
  2 threads and 100 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   682.35us    0.98ms  31.44ms   99.02%
    Req/Sec    70.68k    11.48k  192.91k    91.04%
  1413493 requests in 10.10s, 130.76MB read
  Non-2xx or 3xx responses: 1413493
Requests/sec: 139920.06
Transfer/sec:     12.94M
```

```
minikeyvalue git:(master) ✗ wrk -t2 -c100 -d10s http://localhost:3000/key

Running 10s test @ http://localhost:3000/key
  2 threads and 100 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.22ms    3.50ms  66.29ms   93.28%
    Req/Sec    91.04k    33.44k  201.23k    75.38%
  1816291 requests in 10.08s, 142.04MB read
  Non-2xx or 3xx responses: 1816291
Requests/sec: 180167.17
Transfer/sec:     14.09MB
```

Some small stats, Claude3.5 generated, report if anything is wrong, lgtm.
- **Latency improvements:**
  - Average latency: 44.07% lower
  - Standard deviation: 72.00% lower
  - Max latency: 52.57% lower

- **Requests per second:**
  - Our Rust implementation handles 22.34% fewer requests per second in this specific benchmark


Also when I tested a heavy read scenario where our project exceeds:

Our implementation:
````
rust-minikeyvalue git:(master) ✗ go run tools/thrasher-read.go
starting thrasher
Starting GET round 1 of 1000
Starting GET round 101 of 1000
Starting GET round 201 of 1000
Starting GET round 301 of 1000
Starting GET round 401 of 1000
Starting GET round 501 of 1000
Starting GET round 601 of 1000
Starting GET round 701 of 1000
Starting GET round 801 of 1000
Starting GET round 901 of 1000
Completed 1000 PUTs and 1000000 GETs (1000 rounds) in 36.52528875s
Total operations: 1001000, that's 27405.67 ops/sec
```

Go code crashes! Try to run the test multiple times you will see with 1M GET request it usually fails.

```
minikeyvalue git:(master) ✗ go run tools/thrasher-read.go 
starting thrasher
Starting GET round 1 of 1000
Starting GET round 101 of 1000
Starting GET round 201 of 1000
Starting GET round 301 of 1000
Starting GET round 401 of 1000
Starting GET round 501 of 1000
Starting GET round 601 of 1000
Starting GET round 701 of 1000
GET FAILED Get "http://localhost:3003/sv04/ba/3d/L2JlbmNobWFyay0xODA1MDI5MTk1NjU3NDg3MTAy": read tcp [::1]:56978->[::1]:3003: read: socket is not connected
ERROR on GET, round 745, key 164
exit status 255
```