Jzab
=======

[![Build Status](https://travis-ci.org/zk1931/jzab.svg?branch=master)](https://travis-ci.org/zk1931/jzab)

`jzab` is an implementation of 
[ZooKeeper Atomic Broadcast](http://web.stanford.edu/class/cs347/reading/zab.pdf) (Zab)
in Java. `jzab`'s features include:

- **High throughput** - [benchmarked > 20k writes/sec](https://github.com/zk1931/jzab/wiki) on commodity hardware.
- **Fuzzy snapshot** - minimizes service interruption while taking snapshots.
- **Dynamic reconfiguration** - add/remove servers without restarting the cluster.
- **Minimum runtime dependencies** - netty, protobuf, slf4j.
- **Secure communication** - using ssl.

The latest documentation from the master branch: http://zk1931.github.io/jzab/master/

Applications using `jzab`
-------------------------
- [`zabkv`](https://github.com/zk1931/zabkv) - A simple reference server.
- [`pulsed`](https://github.com/zk1931/pulsed) - An HTTP-based replicated filestore for distributed coordination.

Requirements
------------
 - JDK 1.7 or later: `javac -version`
 - Maven 3 or later: `mvn -v`
 - Protocol Buffers compiler 2.4 or later: `protoc --version`

How to build
------------

To build the project, run:

    mvn verify
