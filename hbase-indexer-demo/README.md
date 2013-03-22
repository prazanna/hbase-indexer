# HBase Indexer Demo

The demo consists of:
 * tools to upload data into HBase
 * matching indexer configurations

The indexer configurations assume the default Solr schema is used.

There is a demo for row and for column based indexing.

##Row-based indexing demo

Add an indexer (as usual with "hbase-indexer add-indexer") using the
configuration message_indexer.xml

Start the data upload process (this will also create the schema in hbase)
with:

    ./bin/hbase-indexer com.ngdata.hbaseindexer.demo.DemoUserIngester

By default one record at a time will be put. You can increase the throughput
using options like:

    ./bin/hbase-indexer com.ngdata.hbaseindexer.demo.DemoUserIngester -batchsize 100 -threads 10

The batchsize option will use multi-puts on HBase.

##Column-based indexing demo

Add an indexer using the configuration from either message_indexer.xml
or tika_message_indexer.xml

Start the data upload process (this will also create the schema in hbase)
with:

    ./bin/hbase-indexer com.ngdata.hbaseindexer.demo.DemoMessageIngester

By default this will add one mesasge at a time, the messages are spread over
1000 users. You can use a different number of users, and more threads, and
HBase multi-put using:

    ./bin/hbase-indexer com.ngdata.hbaseindexer.demo.DemoMessageIngester -users 10000 -batchsize 50 -threads 10