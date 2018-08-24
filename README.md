# Cuckoo Filter Performance Analyzer

This code base tests Cuckoo Filter performance in Cassandra. There are two levels of tests performed:
- `SSTable`-level Cuckoo Filter performance tests. The test suite is the [`Main`](Main.java) class. The tests compare performance between Cuckoo Filter and Bloom Filter.
- Node-level Cuckoo Filter performance tests. The test suite is the [`MainCluster`](MainCluster.java) class. The tests compare performance among the default implementation (i.e. without node-level filters), Node Cuckoo Filter and Node Bloom Filter.
