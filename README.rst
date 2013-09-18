==========
Silverberg
==========

|build|_ |coverage|_

Silverberg is a connection pooled, low-level client API for Cassandra CQL3 in Twisted python.

Named after the SSS Silverberg, which contained the computer Cassandra in an episode of Red Dwarf.  Hey, all of the mythological references to Cassandra are all getting kinda old, no?

CQL3 is the new query language for Apache Cassandra 1.2 and onward.  You can use it almost, but not quite, like SQL from a traditional relational database.

CQL3 reference
==============

https://cassandra.apache.org/doc/cql3/CQL.html

Installation
============

``pip install silverberg``

Prerequisites:

* Python >= 2.7
* Twisted
* Thrift
* Cassandra >= 1.2

Version History
===============

- 0.1.5
    -  Different claim ids are logged when lock is not acquired
    -  Removed logging of intermittent 'Released lock' messages that occurred when
       lock acquisition was tried again
    -  Logging node info when connection to a node is lost in an unclean fashion
- 0.1.4
    -  LoggingCQLClient logs cql failures as msg
    -  ``null`` values unmarshal correctly now, no matter what the
       type, because ``null`` values will just always be unmarshalled as None
    -  BasicLock takes optional log argument that if given logs when the lock was acquired and released
       along with time taken
- 0.1.3
    -  RoundRobinCassandraCluster tries the next node in the cluster if it gets conection error
    -  LoggingCQLClient class implemented that will log every CQL query, parameters and seconds taken to
       execute the query
    -  Fixed bug in locks recipe where it didn't get row in some rare scenarios
    -  Fix marshalling of counter
    -  Issue with how round robin cluster keeps track of rotating client
- 0.1.2
    - Fix marshalling of timestamps
- 0.1.1
    - Adding locks recipe for cassandra-based named locks
- 0.1.0
    - Changed API, cleaned up return valeus
- 0.0.x
    - Early development version

Running Tests and Lint
======================

``make test`` and ``make lint``

License
=======

Silverberg is distributed under the Apache license v2.0.  See LICENSE.txt

Contributing
============

We love pull requests!  Please:

* Follow reasonable GitHub Pull Request practices
* Make sure that your new contributed code contains reasonable unit tests
* Unit tests and lint continue to pass


.. |build| image:: https://secure.travis-ci.org/rackerlabs/silverberg.png?branch=master
.. _build: http://travis-ci.org/rackerlabs/silverberg

.. |coverage| image:: https://coveralls.io/repos/rackerlabs/silverberg/badge.png?branch=master
.. _coverage: https://coveralls.io/r/rackerlabs/silverberg?branch=master
