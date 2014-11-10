---
layout: page
title: Jzab
---

## What's Zab

[Zab](http://web.stanford.edu/class/cs347/reading/zab.pdf) is a
atomic broadcast protocol designed for the [Apache ZooKeeper]
(http://zookeeper.apache.org/). ZooKeeper uses Zab to replicate its in-memory
state to a set of backups consistently to provide high availability.
It has similar goal with
[Paxos](http://en.wikipedia.org/wiki/Paxos_\(computer_science\)) and
[Raft](http://raftconsensus.github.io/) with high performance.

## Jzab

Since ZooKeeper's Zab implementation is rather entangled with ZooKeeper code
base, we implement [Jzab](https://github.com/zk1931/jzab) as an independent
implementation of Zab protocol to facilitate other users who want to build
applications on top of it.

### Features

* Channel encryption and authentication (SSL)
* Dynamic reconfiguration (Add/Remove one server at a time)
* Snapshot

### Talk

[Slides](http://www.slideshare.net/easonliao315/zk-meetup-talk-41241692)
for ZooKeeper meetup.

### Install

Jzab is available on Maven.If your project uses Maven, please add these
lines to your pom.xml.

{% highlight xml %}
<dependency>
    <groupId>com.github.zk1931</groupId>
    <artifactId>jzab</artifactId>
    <version>{{ site.version }}</version>
</dependency>
{% endhighlight %}
