/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zab.transport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.zab.TestBase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test NettyTransport.
 */
public class NettyTransportTest extends TestBase {
  private static final Logger LOG = LoggerFactory
                                    .getLogger(NettyTransportTest.class);

  private static final Transport.Receiver NOOP = new Transport.Receiver() {
    @Override
    public void onReceived(String source, ByteBuffer message) {
    }

    @Override
    public void onDisconnected(String source) {
    }
  };

  public static ByteBuffer createByteBuffer(int num) {
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(num);
    bb.flip();
    return bb;
  }

  /**
   * Make sure the constructor fails when the port is invalid.
   */
  @Test(timeout=5000, expected=IllegalArgumentException.class)
  public void testInvalidPort() throws Exception {
    NettyTransport transport = new NettyTransport(getHostPort(-1), NOOP);
  }

  @Test(timeout=1000)
  public void testLocalSend() throws Exception {
    final String localId = getUniqueHostPort();

    // receiver simply appends messages to a list.
    final LinkedList<ByteBuffer> messages = new LinkedList<>();
    Transport.Receiver receiver = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
        Assert.assertEquals(localId, source);
        messages.add(message);
      }
      public void onDisconnected(String source) {
      }
    };

    // send messages to itself.
    NettyTransport transport = new NettyTransport(localId, receiver);
    for (int i = 0; i < 20; i++) {
      transport.send(localId, createByteBuffer(i));
    }

    // receive messages.
    for (int i = 0; i < 20; i++) {
      int message = messages.pop().getInt();
      LOG.debug("Received a message: {}", message);
      Assert.assertEquals(i, message);
    }
    Assert.assertTrue(messages.isEmpty());
  }

  @Test(timeout=5000)
  public void testConnectFailed() throws Exception {
    final String peerA = getUniqueHostPort();
    final String peerB = getUniqueHostPort();
    final CountDownLatch disconnected = new CountDownLatch(1);
    Transport.Receiver receiverA = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
      }
      public void onDisconnected(String source) {
        disconnected.countDown();
      }
    };
    NettyTransport transportA = new NettyTransport(peerA, receiverA);
    transportA.send(peerB, createByteBuffer(0));
    disconnected.await();
  }

  @Test(timeout=5000)
  public void testSend() throws Exception {
    final String peerA = getUniqueHostPort();
    final String peerB = getUniqueHostPort();

    // receiver simply appends messages to a list.
    int messageCount = 20;
    final LinkedList<ByteBuffer> messagesA = new LinkedList<>();
    final LinkedList<ByteBuffer> messagesB = new LinkedList<>();

    final CountDownLatch latchA = new CountDownLatch(messageCount);
    final CountDownLatch latchB = new CountDownLatch(messageCount);

    Transport.Receiver receiverA = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
        Assert.assertEquals(peerB, source);
        messagesA.add(message);
        latchA.countDown();
      }
      public void onDisconnected(String source) {
      }
    };
    Transport.Receiver receiverB = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
        Assert.assertEquals(peerA, source);
        messagesB.add(message);
        latchB.countDown();
      }
      public void onDisconnected(String source) {
      }
    };

    NettyTransport transportA = new NettyTransport(peerA, receiverA);
    NettyTransport transportB = new NettyTransport(peerB, receiverB);

    // send messages from A to B.
    for (int i = 0; i < messageCount; i++) {
      transportA.send(peerB, createByteBuffer(i));
    }
    latchB.await();
    for (int i = 0; i < messageCount; i++) {
      int message = messagesB.pop().getInt();
      LOG.debug("Received a message: {}", message);
      Assert.assertEquals(i, message);
    }
    Assert.assertTrue(messagesB.isEmpty());

    // send messages from B to A.
    for (int i = 0; i < messageCount; i++) {
      transportB.send(peerA, createByteBuffer(i));
    }
    latchA.await();
    for (int i = 0; i < messageCount; i++) {
      int message = messagesA.pop().getInt();
      LOG.debug("Received a message: {}", message);
      Assert.assertEquals(i, message);
    }
    Assert.assertTrue(messagesA.isEmpty());
    transportA.shutdown();
    transportB.shutdown();
  }

  @Test(timeout=5000)
  public void testDisconnectClient() throws Exception {
    final String peerA = getUniqueHostPort();
    final String peerB = getUniqueHostPort();
    final CountDownLatch latchA = new CountDownLatch(1);
    final CountDownLatch latchB = new CountDownLatch(1);
    final CountDownLatch disconnectedB = new CountDownLatch(1);

    // receiver simply decrement the latch.
    Transport.Receiver receiverA = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
        Assert.assertEquals(peerB, source);
        latchA.countDown();
      }
      public void onDisconnected(String source) {
      }
    };
    Transport.Receiver receiverB = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
        Assert.assertEquals(peerA, source);
        latchB.countDown();
      }
      public void onDisconnected(String source) {
        disconnectedB.countDown();
      }
    };
    NettyTransport transportA = new NettyTransport(peerA, receiverA);
    NettyTransport transportB = new NettyTransport(peerB, receiverB);

    // A initiates a handshake.
    transportA.send(peerB, createByteBuffer(0));
    latchB.await();

    // shutdown A and make sure B removes the channel to A.
    transportA.shutdown();
    Assert.assertTrue(transportA.senders.isEmpty());
    disconnectedB.await();
    Assert.assertTrue(transportB.senders.containsKey(peerA));
    transportB.shutdown();
  }

  @Test(timeout=5000)
  public void testDisconnectServer() throws Exception {
    final String peerA = getUniqueHostPort();
    final String peerB = getUniqueHostPort();
    final CountDownLatch latchA = new CountDownLatch(1);
    final CountDownLatch latchB = new CountDownLatch(1);

    // receiver simply decrement the latch.
    Transport.Receiver receiverA = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
      }
      public void onDisconnected(String source) {
        latchA.countDown();
      }
    };
    Transport.Receiver receiverB = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
        Assert.assertEquals(peerA, source);
        latchB.countDown();
      }
      public void onDisconnected(String source) {
      }
    };
    NettyTransport transportA = new NettyTransport(peerA, receiverA);
    NettyTransport transportB = new NettyTransport(peerB, receiverB);

    // A initiates a handshake.
    transportA.send(peerB, createByteBuffer(0));
    latchB.await();

    // shutdown B and make sure A removes the channel to B.
    transportB.shutdown();
    Assert.assertTrue(transportB.senders.isEmpty());

    // A should get onDisconnected event, but B should still be in the map.
    latchA.await();
    Assert.assertTrue(transportA.senders.containsKey(peerB));
    transportA.shutdown();
  }

  @Test(timeout=5000)
  public void testTieBreaker() throws Exception {
    final String peerA = getUniqueHostPort();
    final String peerB = getUniqueHostPort();
    final int messageCount = 100;
    final CountDownLatch latchA = new CountDownLatch(messageCount);
    final CountDownLatch disconnectedA = new CountDownLatch(1);
    final CountDownLatch latchB = new CountDownLatch(messageCount);

    // receivers simply decrement the latch.
    Transport.Receiver receiverA = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
        LOG.debug("Received a message from {}: {}", source, message);
        Assert.assertEquals(peerB, source);
        latchA.countDown();
      }
      public void onDisconnected(String source) {
        LOG.debug("Got disconnected from {}", source);
        disconnectedA.countDown();
      }
    };
    Transport.Receiver receiverB = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
        Assert.assertEquals(peerA, source);
        latchB.countDown();
        LOG.debug("Received a message from {}: {}: {}", source, message,
                  latchB.getCount());
      }
      public void onDisconnected(String source) {
        LOG.debug("Got disconnected from {}", source);
      }
    };

    final NettyTransport transportA = new NettyTransport(peerA, receiverA);
    final NettyTransport transportB = new NettyTransport(peerB, receiverB);
    transportB.channel.pipeline().addFirst(
      new ChannelInboundHandlerAdapter() {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
          // B initiates another handshake before responding to A's handshake.
          for (int i = 0; i < messageCount; i++) {
            transportB.send(peerA, createByteBuffer(i));
          }
          ctx.pipeline().remove(this);
          ctx.fireChannelRead(msg);
        }
      });

    // A initiates a handshake.
    for (int i = 0; i < messageCount; i++) {
      transportA.send(peerB, createByteBuffer(i));
    }
    latchA.await();
    latchB.await();
    transportA.shutdown();
    transportB.shutdown();
  }

  @Test(timeout=5000)
  public void testCloseClient() throws Exception {
    final String peerA = getUniqueHostPort();
    final String peerB = getUniqueHostPort();
    final int messageCount = 100;
    final CountDownLatch latchB = new CountDownLatch(messageCount);
    final CountDownLatch disconnectedB = new CountDownLatch(1);

    Transport.Receiver receiverA = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
      }
      public void onDisconnected(String source) {
      }
    };
    Transport.Receiver receiverB = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
        Assert.assertEquals(peerA, source);
        latchB.countDown();
        LOG.debug("Received a message from {}: {}: {}", source, message,
                  latchB.getCount());
      }
      public void onDisconnected(String source) {
        LOG.debug("Got disconnected from {}", source);
        disconnectedB.countDown();
      }
    };

    final NettyTransport transportA = new NettyTransport(peerA, receiverA);
    final NettyTransport transportB = new NettyTransport(peerB, receiverB);

    for (int i = 0; i < messageCount; i++) {
      transportA.send(peerB, createByteBuffer(i));
    }
    latchB.await();

    // A should remove B from the map after clear() is called.
    transportA.clear(peerB);
    Assert.assertFalse(transportA.senders.containsKey(peerB));

    // B should get onDisconnected event, but A should be in the map.
    disconnectedB.await();
    Assert.assertTrue(transportB.senders.containsKey(peerA));
    transportA.shutdown();
    transportB.shutdown();
  }

  @Test(timeout=5000)
  public void testCloseServer() throws Exception {
    final String peerA = getUniqueHostPort();
    final String peerB = getUniqueHostPort();
    final int messageCount = 100;
    final CountDownLatch latchB = new CountDownLatch(messageCount);
    final CountDownLatch disconnectedA = new CountDownLatch(1);
    final CountDownLatch disconnectedB = new CountDownLatch(1);

    Transport.Receiver receiverA = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
      }
      public void onDisconnected(String source) {
        LOG.debug("Got disconnected from {}", source);
        disconnectedA.countDown();
      }
    };
    Transport.Receiver receiverB = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
        Assert.assertEquals(peerA, source);
        latchB.countDown();
        LOG.debug("Received a message from {}: {}: {}", source, message,
                  latchB.getCount());
      }
      public void onDisconnected(String source) {
      }
    };

    final NettyTransport transportA = new NettyTransport(peerA, receiverA);
    final NettyTransport transportB = new NettyTransport(peerB, receiverB);

    for (int i = 0; i < messageCount; i++) {
      transportA.send(peerB, createByteBuffer(i));
    }
    latchB.await();

    // B should remove A from the map after clear() is called.
    transportB.clear(peerA);
    Assert.assertFalse(transportB.senders.containsKey(peerA));

    // A should get onDisconnected event, but B should be in the map.
    disconnectedA.await();
    Assert.assertTrue(transportA.senders.containsKey(peerB));

    transportA.shutdown();
    transportB.shutdown();
  }

  @Test(timeout=10000)
  public void testHandshakeTimeout() throws Exception {
    final String peerA = getUniqueHostPort();
    final String peerB = getUniqueHostPort();
    final CountDownLatch disconnectedA = new CountDownLatch(1);

    Transport.Receiver receiverA = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
      }
      public void onDisconnected(String source) {
        LOG.debug("Got disconnected from {}", source);
        disconnectedA.countDown();
      }
    };
    Transport.Receiver receiverB = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
      }
      public void onDisconnected(String source) {
      }
    };

    final NettyTransport transportA = new NettyTransport(peerA, receiverA);
    final NettyTransport transportB = new NettyTransport(peerB, receiverB);

    // Discard the handshake message.
    transportB.channel.pipeline().addFirst(
      new ChannelInboundHandlerAdapter() {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
          // discard the message.
        }
      });

    transportA.send(peerB, createByteBuffer(0));
    disconnectedA.await();
    transportA.shutdown();
    transportB.shutdown();
  }

  @Test(timeout=5000)
  public void testBroadcast() throws Exception {
    final int messageCount = 100;
    final String peerA = getUniqueHostPort();
    final String peerB = getUniqueHostPort();
    final String peerC = getUniqueHostPort();
    final CountDownLatch latchA = new CountDownLatch(messageCount * 2);
    final CountDownLatch latchB = new CountDownLatch(messageCount * 2);
    final CountDownLatch latchC = new CountDownLatch(messageCount * 2);
    Transport.Receiver receiverA = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
        latchA.countDown();
        LOG.debug("A :{} {} {}", source, message.getInt(), latchA.getCount());
      }
      public void onDisconnected(String source) {
      }
    };
    Transport.Receiver receiverB = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
        latchB.countDown();
        LOG.debug("B :{} {} {}", source, message.getInt(), latchB.getCount());
      }
      public void onDisconnected(String source) {
      }
    };
    Transport.Receiver receiverC = new Transport.Receiver() {
      public void onReceived(String source, ByteBuffer message) {
        latchC.countDown();
        LOG.debug("C :{} {} {}", source, message.getInt(), latchC.getCount());
      }
      public void onDisconnected(String source) {
      }
    };
    final NettyTransport transportA = new NettyTransport(peerA, receiverA);
    final NettyTransport transportB = new NettyTransport(peerB, receiverB);
    final NettyTransport transportC = new NettyTransport(peerC, receiverC);

    Callable<Void> broadcastA = new Callable<Void>() {
      public Void call() throws Exception {
        for (int i = 0; i < messageCount; i++) {
          transportA.broadcast(Arrays.asList(peerB, peerC).listIterator(),
                               createByteBuffer(i));
        }
        return null;
      }
    };
    Callable<Void> broadcastB = new Callable<Void>() {
      public Void call() throws Exception {
        for (int i = 0; i < messageCount; i++) {
          transportB.broadcast(Arrays.asList(peerA, peerC).listIterator(),
                               createByteBuffer(i));
        }
        return null;
      }
    };
    Callable<Void> broadcastC = new Callable<Void>() {
      public Void call() throws Exception {
        for (int i = 0; i < messageCount; i++) {
          transportC.broadcast(Arrays.asList(peerA, peerB).listIterator(),
                               createByteBuffer(i));
        }
        return null;
      }
    };
    ExecutorService service = Executors.newFixedThreadPool(3);
    Future<Void> futureA = service.submit(broadcastA);
    Future<Void> futureB = service.submit(broadcastB);
    Future<Void> futureC = service.submit(broadcastC);
    latchA.await();
    latchB.await();
    latchC.await();
    futureA.get();
    futureB.get();
    futureC.get();
    transportA.shutdown();
    transportB.shutdown();
    transportC.shutdown();
  }
}
