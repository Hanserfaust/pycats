pycats
======

Implementation of sharded time series storage built on the Python library Pycassa for Apache Cassandra.

Mainly based on the ideas from various blogs and documentation around the net, for example:
 - http://rubyscale.com/blog/2011/03/06/basic-time-series-with-cassandra/
 - http://www.datastax.com/dev/blog/advanced-time-series-with-cassandra

Not recommended for production use at this time.


ISSUES
====
- Improve key-generation for indexes and blobs also to use the high resolution keys if possible.
- Move blob-storage and indexing out of the TimeSeriesDAO? Or at least change the name of the TimeSeriesDAO

FACADES
=======
While pycats is usable right away. After developing in at using it i´ve found that building a thin facade layer
above the DAO is the way to go. Different facade-instances can make use of different DAOs (hence you have the
ability to run your facades vs. separate keyspaces, since replication can only be configured per key-space)

The CassandraLogger is an example of a facade implementation.

DISCLAIMER
==========
Simple implementation of a few ideas around time series storage on Apache Cassandra. Many improvements
can be made and few optimizations have been made so far. We hope to improve it.

Run the tests to assert functionality.

CHANGELOG
=========

31 Jan, 2013
First version. Should be considered Alpha version, use at own risk with caution.