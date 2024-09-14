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
* **Example**: `curl -v -L localhost:3000/wehave`

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



