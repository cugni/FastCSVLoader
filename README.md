FastCSVLoader
=============

A "fast" plain file loader for Cassandra.
This implementation uses the Disruptor pattern and batches insertions in order to increase the throughput. It supports CSV file and any other kind of character separated value.



###Configurable properties: 

  - cassandra.servers : a comma separated list with the names of the Cassandra entry points. e.g "server1,server2"
  - cassandra.cluster-name: the name of the cluster
  - FS : Field separator, the value used to distinguish between a field and another. Usually is a ","
  - cassandra.port: The Cassandra listening port. The default value is 9042
  - disruptor.consumers: Number of concurrent consumers that insert in parallel. This number must be a power of 2 (2,4,8,16,32....)
