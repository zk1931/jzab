---
layout: page
title: Getting started
---

Jzab is designed for hiding the complexities of underlying Zab protocol and
providing an easy-to-use user interface, but a brief understanding of Zab
protocol will help you use Jzab correctly.

### Overview

Here is a simple diagram of how an application works with Jzab. You can use
Jzab to replicate state of the application consistently, which means that
transactions get applied to all replicas in the same order. Transactions get
persisted (i.e. fdatasync'ed) to a majority of servers before they get
acknowledged to the application.

<img align="middle" src="/imgs/overview.png"/>

You can write the replicated application as easy as writing a standalone
application. The only difference is that whenever you want to execute a command
that will change the state of your application, instead of executing it
immediately, you pass it to Jzab and Jzab will replicate it and deliver it back
to your application when it's safe to execute.

Another thing you need to notice is that there are three states that Jzab might
be in. They are `recovering`, `leading` and `following`. When Jzab is in
`recovering` phase, any requests you passed to it will fail. Jzab will notify you
the state change via callbacks (See interface [StateMachine]
(http://zk1931.github.io/jzab/master/javadoc/com/github/zk1931/jzab/StateMachine.html)).

<img align="middle" src="/imgs/state_transition.png"/>

This is the state transition diagram of Jzab. It begins with ```recovering```
state, afte that it goes to either ```leading``` or ```following``` state depends
on whether it's follower or leader. These two are the states Jzab will spend
most of its time in. Once leader dies or there's no quorum of servers in
cluster, Zab will go back to ```recovering``` state again.

### Example

This is the [Javadoc](http://zk1931.github.io/jzab/master/javadoc/) from latest
master branch build.

In general, if you want to build application on top of Jzab, you only need to
know one class `Zab` and implement one interface `StateMachine`.

Here is a simple example:

{% highlight java %}
/**
 * StateMachine interface contains a bunch of callbacks which will be called
 * by Zab.
 */
class AppState implements StateMachine {

  private final CountDownLatch broadcasting;

  public AppState(CountDownLatch broadcasting) {
    this.broadcasting = broadcasting;
  }

  @Override
  public void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId) {
    byte[] bytes = new byte[stateUpdate.remaining()];
    stateUpdate.get(bytes);
    System.out.println("Received " + new String(bytes));
  }

  @Override
  public void save(OutputStream os) {
  }

  @Override
  public void restore(InputStream is) {
  }

  @Override
  public void flushed(ByteBuffer request) {
  }

  @Override
  public ByteBuffer preprocess(Zxid zxid, ByteBuffer message) {
    // Does nothing.
    return message;
  }

  @Override
  public void leading(Set<String> activeFollowers, Set<String> members) {
    System.out.println("LEADING with active followers : ");
    for (String server : activeFollowers) {
      System.out.println(" - " + server);
    }
    System.out.println("Current configuration has servers : ");
    for (String server : members) {
      System.out.println(" - " + server);
    }
    this.broadcasting.countDown();
  }

  @Override
  public void following(String leader, Set<String> members) {
    System.out.println("Following " + leader);
    System.out.println("Current configuration has servers : ");
    for (String server : members) {
      System.out.println(" - " + server);
    }
    this.broadcasting.countDown();
  }

  @Override
  public void recovering() {
    System.out.println("Recovering");
  }
}

/**
 * Main.
 */
public class Main {

  protected Main() {}

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("Invalid arguments!");
      return;
    }
    String serverId = args[0];
    String joinPeer = serverId;
    if (args.length == 2) {
      joinPeer = args[1];
    }
    CountDownLatch broadcasting = new CountDownLatch(1);
    AppState stateMachine = new AppState(broadcasting);
    Properties prop = new Properties();
    prop.setProperty("serverId", serverId);
    prop.setProperty("logdir", serverId);
    Zab zab = new Zab(stateMachine, prop, joinPeer);
    // Waits until broadcasting.
    broadcasting.await();
    // Broadcasts "HelloWord".
    zab.send(ByteBuffer.wrap(("HelloWorld from " + serverId).getBytes()));
    while (true) {
      Thread.sleep(1000);
    }
  }
}
{% endhighlight %}

You can start 3 servers to form a cluster (The second and third server will
join the first one):

```
  java Main localhost:5000
  java Main localhost:5001 localhost:5000
  java Main localhost:5002 localhost:5000
```

You'll see the output from all three processes like :

```
Received HelloWorld from localhost:5000
Received HelloWorld from localhost:5001
Received HelloWorld from localhost:5002

```

