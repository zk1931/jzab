/**
 * Licensed to the zk1931 under one or more contributor license
 * agreements. See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the
 * License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.zk1931.jzab;

import com.google.protobuf.TextFormat;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import com.github.zk1931.jzab.proto.ZabMessage.Message;
import com.github.zk1931.jzab.proto.ZabMessage.Message.MessageType;
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
    public void onReceived(String source, Message message) {
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

  public static Message createAck(Zxid zxid) {
    return MessageBuilder.buildAck(zxid);
  }

  /**
   * Make sure the constructor fails when the port is invalid.
   */
  @Test(timeout=10000, expected=IllegalArgumentException.class)
  public void testInvalidPort() throws Exception {
    NettyTransport transport = new NettyTransport(getHostPort(-1), NOOP,
                                                  getDirectory());
  }

  @Test(timeout=1000)
  public void testLocalSend() throws Exception {
    final String localId = getUniqueHostPort();

    // receiver simply appends messages to a list.
    final LinkedList<Zxid> messages = new LinkedList<>();
    Transport.Receiver receiver = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
        Assert.assertEquals(localId, source);
        Zxid zxid = MessageBuilder.fromProtoZxid(message.getAck().getZxid());
        messages.add(zxid);
      }
      public void onDisconnected(String source) {
      }
    };

    // send messages to itself.
    NettyTransport transport = new NettyTransport(localId, receiver,
                                                  getDirectory());
    for (int i = 0; i < 20; i++) {
      transport.send(localId, createAck(new Zxid(0, i)));
    }

    // receive messages.
    for (int i = 0; i < 20; i++) {
      Zxid zxid = messages.pop();
      LOG.debug("Received a message: {}", zxid);
      Assert.assertEquals(new Zxid(0, i), zxid);
    }
    Assert.assertTrue(messages.isEmpty());
  }

  @Test(timeout=5000)
  public void testConnectFailed() throws Exception {
    final String peerA = getUniqueHostPort();
    final String peerB = getUniqueHostPort();
    final CountDownLatch disconnected = new CountDownLatch(1);
    Transport.Receiver receiverA = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
      }
      public void onDisconnected(String source) {
        disconnected.countDown();
      }
    };
    NettyTransport transportA = new NettyTransport(peerA, receiverA,
                                                   getDirectory());
    transportA.send(peerB, createAck(new Zxid(0, 0)));
    disconnected.await();
    Assert.assertTrue(transportA.senders.containsKey(peerB));
    Assert.assertFalse(transportA.receivers.containsKey(peerB));
  }

  @Test(timeout=10000)
  public void testSend() throws Exception {
    final String peerA = getUniqueHostPort();
    final String peerB = getUniqueHostPort();

    // receiver simply appends messages to a list.
    int messageCount = 20;
    final LinkedList<Zxid> messagesA = new LinkedList<>();
    final LinkedList<Zxid> messagesB = new LinkedList<>();

    final CountDownLatch latchA = new CountDownLatch(messageCount);
    final CountDownLatch latchB = new CountDownLatch(messageCount);

    Transport.Receiver receiverA = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
        Assert.assertEquals(peerB, source);
        Zxid zxid = MessageBuilder.fromProtoZxid(message.getAck().getZxid());
        messagesA.add(zxid);
        latchA.countDown();
      }
      public void onDisconnected(String source) {
      }
    };
    Transport.Receiver receiverB = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
        Assert.assertEquals(peerA, source);
        Zxid zxid = MessageBuilder.fromProtoZxid(message.getAck().getZxid());
        messagesB.add(zxid);
        latchB.countDown();
      }
      public void onDisconnected(String source) {
      }
    };

    NettyTransport transportA = new NettyTransport(peerA, receiverA,
                                                   getDirectory());
    NettyTransport transportB = new NettyTransport(peerB, receiverB,
                                                   getDirectory());

    // send messages from A to B.
    for (int i = 0; i < messageCount; i++) {
      transportA.send(peerB, createAck(new Zxid(0, i)));
    }
    latchB.await();
    for (int i = 0; i < messageCount; i++) {
      Zxid zxid  = messagesB.pop();
      LOG.debug("Received a message: {}", zxid);
      Assert.assertEquals(new Zxid(0, i), zxid);
    }
    Assert.assertTrue(messagesB.isEmpty());

    // send messages from B to A.
    for (int i = 0; i < messageCount; i++) {
      transportB.send(peerA, createAck(new Zxid(0, i)));
    }
    latchA.await();
    for (int i = 0; i < messageCount; i++) {
      Zxid zxid  = messagesA.pop();
      LOG.debug("Received a message: {}", zxid);
      Assert.assertEquals(new Zxid(0, i), zxid);
    }
    Assert.assertTrue(messagesA.isEmpty());
    transportA.shutdown();
    transportB.shutdown();
  }

  @Test(timeout=10000)
  public void testClear() throws Exception {
    final String peerA = getUniqueHostPort();
    final String peerB = getUniqueHostPort();
    final CountDownLatch latchA = new CountDownLatch(1);
    final CountDownLatch latchB = new CountDownLatch(1);
    final CountDownLatch latchReceiveB = new CountDownLatch(1);

    Transport.Receiver receiverA = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
      }
      public void onDisconnected(String source) {
        latchA.countDown();
      }
    };
    Transport.Receiver receiverB = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
        latchReceiveB.countDown();
      }
      public void onDisconnected(String source) {
        latchB.countDown();
      }
    };
    NettyTransport transportA = new NettyTransport(peerA, receiverA,
                                                   getDirectory());
    NettyTransport transportB = new NettyTransport(peerB, receiverB,
                                                   getDirectory());
    transportA.send(peerB, createAck(new Zxid(0, 1)));
    // Waits for connection established.
    latchReceiveB.await();
    // Clear B.
    transportA.clear(peerB);
    // B shouldn't receive DISCONNECT message.
    Assert.assertFalse(latchB.await(500, TimeUnit.MILLISECONDS));
    // A shouldn't receive DISCONNECT message.
    Assert.assertFalse(latchA.await(500, TimeUnit.MILLISECONDS));
    transportA.shutdown();
    transportB.shutdown();
  }

  @Test(timeout=5000)
  public void testDisconnectClient() throws Exception {
    final String peerA = getUniqueHostPort();
    final String peerB = getUniqueHostPort();
    final CountDownLatch latchA = new CountDownLatch(1);
    final CountDownLatch latchB = new CountDownLatch(1);

    // receiver simply decrement the latch.
    Transport.Receiver receiverA = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
        Assert.assertEquals(peerB, source);
        latchA.countDown();
      }
      public void onDisconnected(String source) {
      }
    };
    Transport.Receiver receiverB = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
        Assert.assertEquals(peerA, source);
        latchB.countDown();
      }
      public void onDisconnected(String source) {
      }
    };
    NettyTransport transportA =
      new NettyTransport(peerA, receiverA, getDirectory());
    NettyTransport transportB =
      new NettyTransport(peerB, receiverB, getDirectory());

    // A initiates a handshake.
    transportA.send(peerB, createAck(new Zxid(0, 0)));
    latchB.await();
    Assert.assertTrue(transportB.receivers.containsKey(peerA));

    // shutdown A and make sure B removes the channel to A.
    transportA.shutdown();
    Assert.assertTrue(transportA.senders.isEmpty());
    Assert.assertTrue(transportA.receivers.isEmpty());
    Assert.assertFalse(transportB.senders.containsKey(peerA));
    Assert.assertTrue(transportB.receivers.containsKey(peerA));
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
      public void onReceived(String source, Message message) {
      }
      public void onDisconnected(String source) {
        latchA.countDown();
      }
    };
    Transport.Receiver receiverB = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
        Assert.assertEquals(peerA, source);
        latchB.countDown();
      }
      public void onDisconnected(String source) {
      }
    };
    NettyTransport transportA =
      new NettyTransport(peerA, receiverA, getDirectory());
    NettyTransport transportB =
      new NettyTransport(peerB, receiverB, getDirectory());

    // A initiates a handshake.
    transportA.send(peerB, createAck(new Zxid(0, 0)));
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
  public void testCloseClient() throws Exception {
    final String peerA = getUniqueHostPort();
    final String peerB = getUniqueHostPort();
    final int messageCount = 100;
    final CountDownLatch latchB = new CountDownLatch(messageCount);
    final CountDownLatch disconnectedB = new CountDownLatch(1);

    Transport.Receiver receiverA = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
      }
      public void onDisconnected(String source) {
        LOG.debug("Got disconnected from {}", source);
      }
    };
    Transport.Receiver receiverB = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
        Assert.assertEquals(peerA, source);
        latchB.countDown();
        LOG.debug("Received a message from {}: {}: {}", source,
                  TextFormat.shortDebugString(message), latchB.getCount());
      }
      public void onDisconnected(String source) {
        LOG.debug("Got disconnected from {}", source);
        disconnectedB.countDown();
      }
    };

    final NettyTransport transportA =
      new NettyTransport(peerA, receiverA, getDirectory());
    final NettyTransport transportB =
      new NettyTransport(peerB, receiverB, getDirectory());

    for (int i = 0; i < messageCount; i++) {
      transportA.send(peerB, createAck(new Zxid(0, i)));
    }
    latchB.await();
    Assert.assertTrue(transportB.receivers.containsKey(peerA));

    // A should remove B from the map after clear() is called.
    transportA.clear(peerB);
    Assert.assertFalse(transportA.senders.containsKey(peerB));
    Assert.assertFalse(transportA.receivers.containsKey(peerB));

    // B should get onDisconnected event.
    latchB.await();
    Assert.assertFalse(transportB.senders.containsKey(peerA));
    Assert.assertTrue(transportB.receivers.containsKey(peerA));
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
      public void onReceived(String source, Message message) {
      }
      public void onDisconnected(String source) {
        LOG.debug("Got disconnected from {}", source);
        disconnectedA.countDown();
      }
    };
    Transport.Receiver receiverB = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
        Assert.assertEquals(peerA, source);
        latchB.countDown();
        LOG.debug("Received a message from {}: {}: {}", source,
                  TextFormat.shortDebugString(message), latchB.getCount());
      }
      public void onDisconnected(String source) {
        LOG.debug("Got disconnected from {}", source);
        disconnectedA.countDown();
      }
    };

    final NettyTransport transportA =
      new NettyTransport(peerA, receiverA, getDirectory());
    final NettyTransport transportB =
      new NettyTransport(peerB, receiverB, getDirectory());

    for (int i = 0; i < messageCount; i++) {
      transportA.send(peerB, createAck(new Zxid(0, i)));
    }
    latchB.await();

    // B should remove A from the map after clear() is called, but it shouldn't
    // call onDisconnected.
    transportB.clear(peerA);
    Assert.assertFalse(transportB.senders.containsKey(peerA));
    Assert.assertFalse(transportB.receivers.containsKey(peerA));
    Assert.assertFalse(disconnectedB.await(500, TimeUnit.MILLISECONDS));

    // A should get onDisconnected event.
    disconnectedA.await();
    Assert.assertTrue(transportA.senders.containsKey(peerB));
    Assert.assertFalse(transportA.receivers.containsKey(peerB));

    transportA.shutdown();
    transportB.shutdown();
  }

  @Test(timeout=10000)
  public void testBroadcast() throws Exception {
    final int messageCount = 100;
    final String peerA = getUniqueHostPort();
    final String peerB = getUniqueHostPort();
    final String peerC = getUniqueHostPort();
    final CountDownLatch latchA = new CountDownLatch(messageCount);
    final CountDownLatch latchB = new CountDownLatch(messageCount);
    final CountDownLatch latchC = new CountDownLatch(messageCount);
    Transport.Receiver receiverA = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
        latchA.countDown();
      }
      public void onDisconnected(String source) {
      }
    };
    Transport.Receiver receiverB = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
        latchB.countDown();
      }
      public void onDisconnected(String source) {
      }
    };
    Transport.Receiver receiverC = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
        latchC.countDown();
      }
      public void onDisconnected(String source) {
      }
    };
    final NettyTransport transportA =
      new NettyTransport(peerA, receiverA, getDirectory());
    final NettyTransport transportB =
      new NettyTransport(peerB, receiverB, getDirectory());
    final NettyTransport transportC =
      new NettyTransport(peerC, receiverC, getDirectory());

    for (int i = 0; i < messageCount; i++) {
      transportA.broadcast(Arrays.asList(peerA, peerB, peerC).listIterator(),
                           createAck(new Zxid(0, 0)));
    }
    latchA.await();
    latchB.await();
    latchC.await();
    transportA.shutdown();
    transportB.shutdown();
    transportC.shutdown();
  }

  @Test(timeout=20000)
  public void testSsl() throws Exception {
    String peerA = getUniqueHostPort();
    String peerB = getUniqueHostPort();
    String peerC = getUniqueHostPort();
    String peerD = getUniqueHostPort();
    final CountDownLatch latchA = new CountDownLatch(1);
    Transport.Receiver receiverA = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
        latchA.countDown();
      }
      public void onDisconnected(String source) {
      }
    };
    Transport.Receiver receiverB = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
      }
      public void onDisconnected(String source) {
      }
    };
    Transport.Receiver receiverC = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
      }
      public void onDisconnected(String source) {
      }
    };
    Transport.Receiver receiverD = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
      }
      public void onDisconnected(String source) {
      }
    };
    String password = "pa55w0rd";
    String sslDir = "target" + File.separator + "generated-resources" +
                    File.separator + "ssl";

    File trustStore = new File(sslDir, "truststore.jks");
    File keyStoreA = new File(sslDir, "keystore_a.jks");
    File keyStoreB = new File(sslDir, "keystore_b.jks");
    File keyStoreC = new File(sslDir, "keystore_c.jks");

    ZabConfig.SslParameters sslParam1 =
      new ZabConfig.SslParameters(keyStoreA, password, trustStore, password);
    ZabConfig.SslParameters sslParam2 =
      new ZabConfig.SslParameters(keyStoreB, password, trustStore, password);
    ZabConfig.SslParameters sslParam3 =
      new ZabConfig.SslParameters(keyStoreC, password, trustStore, password);

    NettyTransport transportA =
      new NettyTransport(peerA, receiverA, sslParam1, getDirectory());
    NettyTransport transportB =
      new NettyTransport(peerB, receiverB, sslParam2, getDirectory());
    NettyTransport transportC =
      new NettyTransport(peerC, receiverC, sslParam3, getDirectory());
    NettyTransport transportD =
      new NettyTransport(peerD, receiverD, getDirectory());

    // D doesn't use SSL
    transportD.send(peerA, createAck(new Zxid(0, 0)));
    Assert.assertFalse(latchA.await(2, TimeUnit.SECONDS));

    // C uses untrusted cert
    transportC.send(peerA, createAck(new Zxid(0, 0)));
    Assert.assertFalse(latchA.await(2, TimeUnit.SECONDS));

    // B uses trusted cert
    transportB.send(peerA, createAck(new Zxid(0, 0)));
    latchA.await();

    transportA.shutdown();
    transportB.shutdown();
    transportC.shutdown();
  }

  @Test(timeout=10000)
  public void testSendFile() throws Exception {
    int messageCount = 3;
    final String peerA = getUniqueHostPort();
    final String peerB = getUniqueHostPort();
    final CountDownLatch latchA = new CountDownLatch(messageCount);
    final CountDownLatch latchB = new CountDownLatch(messageCount);
    final ArrayList<File> receivedFiles = new ArrayList<>();

    Transport.Receiver receiverA = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
        LOG.debug("onReceived {}", message);
        latchA.countDown();
      }
      public void onDisconnected(String source) {
      }
    };
    Transport.Receiver receiverB = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
        if (message.getType() == MessageType.FILE_RECEIVED) {
          receivedFiles.add(new File(message.getFileReceived().getFullPath()));
        }
        LOG.debug("Received a message from {}: {}: {}", source,
                  TextFormat.shortDebugString(message), latchB.getCount());
        latchB.countDown();
      }
      public void onDisconnected(String source) {
      }
    };
    final NettyTransport transportA =
      new NettyTransport(peerA, receiverA, getDirectory());
    final NettyTransport transportB =
      new NettyTransport(peerB, receiverB, getDirectory());

    transportA.send(peerB, createAck(new Zxid(0, 0)));
    File file = new File("./pom.xml");
    transportA.send(peerB, file);
    transportA.send(peerB, createAck(new Zxid(0, 1)));
    latchB.await();

    Assert.assertTrue(compareFiles(file, receivedFiles.get(0)));
    transportA.shutdown();
    transportB.shutdown();
  }

  @Test(timeout=10000)
  public void testSendFileSsl() throws Exception {
    int messageCount = 3;
    final String peerA = getUniqueHostPort();
    final String peerB = getUniqueHostPort();
    final CountDownLatch latchA = new CountDownLatch(messageCount);
    final CountDownLatch latchB = new CountDownLatch(messageCount);
    final ArrayList<File> receivedFiles = new ArrayList<>();

    String password = "pa55w0rd";
    String sslDir = "target" + File.separator + "generated-resources" +
                    File.separator + "ssl";
    File trustStore = new File(sslDir, "truststore.jks");
    File keyStoreA = new File(sslDir, "keystore_a.jks");
    File keyStoreB = new File(sslDir, "keystore_b.jks");

    Transport.Receiver receiverA = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
        LOG.debug("onReceived {}", message);
        latchA.countDown();
      }
      public void onDisconnected(String source) {
      }
    };
    Transport.Receiver receiverB = new Transport.Receiver() {
      public void onReceived(String source, Message message) {
        if (message.getType() == MessageType.FILE_RECEIVED) {
          receivedFiles.add(new File(message.getFileReceived().getFullPath()));
        }
        LOG.debug("Received a message from {}: {}: {}", source,
                  TextFormat.shortDebugString(message), latchB.getCount());
        latchB.countDown();
      }
      public void onDisconnected(String source) {
      }
    };

    ZabConfig.SslParameters sslParam1 =
      new ZabConfig.SslParameters(keyStoreA, password, trustStore, password);
    ZabConfig.SslParameters sslParam2 =
      new ZabConfig.SslParameters(keyStoreB, password, trustStore, password);

    NettyTransport transportA = new NettyTransport(peerA, receiverA, sslParam1,
                                    getDirectory());
    NettyTransport transportB = new NettyTransport(peerB, receiverB, sslParam2,
                                    getDirectory());
    transportA.send(peerB, createAck(new Zxid(0, 0)));
    File file = new File("./pom.xml");
    transportA.send(peerB, file);
    transportA.send(peerB, createAck(new Zxid(0, 1)));
    latchB.await();

    Assert.assertTrue(compareFiles(file, receivedFiles.get(0)));
    transportA.shutdown();
    transportB.shutdown();
  }

  // Compare if two files are same.
  static boolean compareFiles(File file1, File file2) throws Exception {
    Assert.assertTrue(file1.exists());
    Assert.assertTrue(file2.exists());
    if (file1.length() != file2.length()) {
      return false;
    }
    try (FileInputStream fin1 = new FileInputStream(file1);
         FileInputStream fin2 = new FileInputStream(file2)) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(fin1));
      StringBuilder sb1 = new StringBuilder();
      String line = null;
      while ((line = reader.readLine()) != null) {
        sb1.append(line);
      }
      StringBuilder sb2 = new StringBuilder();
      reader = new BufferedReader(new InputStreamReader(fin2));
      while ((line = reader.readLine()) != null) {
        sb2.append(line);
      }
      return sb1.toString().equals(sb2.toString());
    }
  }
}
