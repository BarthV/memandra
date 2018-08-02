<table style="width:100%">
  <tr>
    <td><img align="center" src="assets/memandra.png" alt="Memandra logo"/></td>
    <td>
      <div text-align="center" align="center">
        <h1>Memandra</h1>
        <h2>A Rend-based Memcached opinionated proxy <br/>using Cassandra as backend</h2>
        <br/>
      </div>
    </td> 
  </tr>
</table>

![Travis CI Badge](https://img.shields.io/travis/BarthV/memandra.svg?style=flat&logo=data:image/png;base64,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) ![Codecov badge](https://img.shields.io/codecov/c/github/BarthV/memandra.svg?style=flat&logo=data:image/png;base64,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)

# Starting Memandra (Memcached gateway for cassandra)

## How To use it ?

`export LISTENPORT=11220 ; export CASSANDRAKEYSPACE=kvstore ; export CASSANDRABUCKET=bucket_1 ; export BUFFERITEMSIZE=10000 ; ./memandra 
`

* only compatible with golang 1.10.x and more
* no CLI options, everything is taken from env vars ... just start it !

Env vars list (and default values) :
```
LISTENPORT" = 11221
METRICSLISTENADDR = ":11299"
CASSANDRAHOST = "127.0.0.1"
CASSANDRAKEYSPACE = "kvstore"
CASSANDRABUCKET = "bucket"
BUFFERITEMSIZE = 80000
BUFFERMAXAGE = "200ms"
BATCHMINSIZE = 1000
BATCHMAXSIZE = 5000
CASSANDRATIMEOUT = "1000ms"
CASSANDRACONNTIMEOUT = "1000ms"
```

Cassandra schema example :
```
CREATE KEYSPACE kvstore WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': '2'}  AND durable_writes = false;

CREATE TABLE kvstore.bucket (
    keycol blob PRIMARY KEY,
    valuecol blob
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 3600
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
```

## Benchmarks
* TODO
