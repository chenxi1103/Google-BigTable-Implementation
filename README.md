# Distributed Bigtable-like Database Implementation
Implement a distributed Bigtable-like database which supports effective CRUD for large-scale data with one master server and multiple tablet servers.

The master server directly communicates with clients through RESTful API and manage the requests by allocating the tasks among tablet servers like table creation and deletion on assigned tablet servers, locking the tables, sharding (splitting of a single table amongst multiple tablet servers for storage effectiveness), health-checking for tablet servers through heartbeat, recovery (handle the permanent loss of a tablet server) based on WAL (write-ahead log) and metadata, and start/kill the servers.

The tablet servers support garbage collection (only keep the last five inserted values), maintaining the memtable for effective searching (memtable would spilled to disk when there are a hundred unique row keys exists), maintaining the in-memory indexes, sharding when a table contains more than 1000 row keys, WAL, reconstructing the tablet server state using WAL and SSTables maintained in the persistent storage, effective range searching in SSTables.
