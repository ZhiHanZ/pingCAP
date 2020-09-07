# Find the first non-repeative word in 100GB file system

## requirements
1. find the first non-repeative word
2. only one time for scanning original file
3. as few IO operations as possible
4. memory restriction: 16GB

## environment setting
if you want to play with this project, you should install go and there are no other dependencies
checking on website for installation details
https://golang.org/doc/install#install

## How to run the program
### build binary file

```bash
make build
```

### run unit tests and e2e tests in program

```
make tests
```
### run a demo

```bash
go run src/cmd/main.go --filename data/demo10000.txt
```

## Design details:
### general idea
for small scale problem, ie if it could fit into memory using a LRU cache structure can resolve the problem.
but here since it is a big data problem and cannot fit into memory, we need to shard the process to multiple files and process them together

1. for every word store them into multiple sharding with key is word it self and value is its frequency and location
2. for every sharding, check all non-repeative word and select the first one given location

### hash partition strategy
to distribute key evenly in all shards, we should adopt general ideas in hash partition
typically hash calculation adopted for partition are murmur3 hash(adopted by cassandra partition)
city hash(google), and spooky hash

### goroutine worker pool scheduler for work distribution and stealing
here I did basic optimization to mitigate the latency during IO transaction.
serialize IO in read
adopted one thread to read line with line number information and send tokens into a channel(workqueue)
worker goroutines will steal unhandled tokens in a greedy approach and update partition
based on their hash bucket
for update and read, because I adopted LSM tree structured persistence Key-value store(badger)
write and read operation is also serialized so all IO operations are handled in a smoothy way
after persistence, I handle each partition separately and divide computing tasks by partition,
for each partition, I will calculate their corresponding first non-repeative word
and aggregate them to achieve the final solution

### key value store persistence layer optimization and why it is fast and thread safe
badger itself supports erializable snapshot isolation (SSI) guarantees which means that except for aggregation operations
all operations will satisfy ACID transation properties, and we can hold the premise that dirty read or non-repeative read
will not happen and isolation can be guaranteed.

### why badger?
#### LSM tree based storage structure

The major performance cost of LSM-trees is the compaction process. During compactions, multiple files are read into memory, sorted, and written back. Sorting is essential for efficient retrieval, for both key lookups and range iterations. With sorting, the key lookups would only require accessing at most one file per level (excluding level zero, where we'd need to check all the files). Iterations would result in sequential access to multiple files.
#### snaller write and read amplification
One drawback of LSM tree is write and read amplification which means value may written multiple times when arranging them in a tree
and a single read may need to read multiple levels in LSM tree before finding the value
badger adopted wiscKey technology to reduce write and read amplification by seprating key and value
in benchmarking:
 For 1KB values and 75 million 22 byte keys, the raw size of the entire dataset is 72 GB. Badger‘s LSM tree size for this setup is a mere 1.7G, which can easily fit into RAM. 
 so if our dataset is for the same size, LSM tree size may need less than 5 GB
  
## ref
https://en.wikipedia.org/wiki/MurmurHash
https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/architecture/archPartitionerM3P.html
http://burtleburtle.net/bob/hash/spooky.html
https://dgraph.io/blog/post/badger-lmdb-boltdb/