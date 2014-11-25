Jzab
=======

An implementation of ZooKeeper Atomic Broadcast in Java

[![Build Status](https://travis-ci.org/zk1931/jzab.svg?branch=master)](https://travis-ci.org/zk1931/jzab)

The latest documentation from the master branch: http://zk1931.github.io/jzab/master/

Requirements
------------
 - JDK 1.7 or later: `javac -version`
 - Maven 3 or later: `mvn -v`
 - Protocol Buffers compiler 2.4 or later: `protoc --version`

How to build
------------

To build the project, run:

    mvn verify

Example
-------

[ZabKV](https://github.com/zk1931/zabkv) - A simple replicated key-value store  built on top of Jzab.
