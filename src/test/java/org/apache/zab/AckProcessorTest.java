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
package org.apache.zab;

import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.zab.proto.ZabMessage.Message;
import org.apache.zab.proto.ZabMessage.Message.MessageType;
import org.apache.zab.transport.NettyTransport;
import org.apache.zab.transport.Transport;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

class TestReceiver implements Transport.Receiver {
  final List<Zxid> committedZxids = new ArrayList<Zxid>();

  final CountDownLatch latch;

  private static final Logger LOG =
    LoggerFactory.getLogger(TestReceiver.class);

  public TestReceiver() {
    latch = null;
  }

  public TestReceiver(int expectedCommits) {
    latch = new CountDownLatch(expectedCommits);
  }

  @Override
  public void onReceived(String source, ByteBuffer message) {
    byte[] buffer = null;
    try {
      // Parses it to protocol message.
      buffer = new byte[message.remaining()];
      message.get(buffer);
      Message msg = Message.parseFrom(buffer);
      if (msg.getType() == MessageType.COMMIT) {
        Zxid zxid = MessageBuilder.fromProtoZxid(msg.getCommit().getZxid());
        LOG.debug("Got COMMIT {} from {}.", zxid, source);
        committedZxids.add(zxid);
        if (latch != null) {
          latch.countDown();
        }
      }
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Invalid protocol buffer message.");
    }
  }

  @Override
  public void onDisconnected(String peerId) {
  }
}

/**
 * Tests for AckProcessor.
 */
public class AckProcessorTest extends TestBase {
  private static final Logger LOG =
    LoggerFactory.getLogger(AckProcessorTest.class);

  private MessageTuple createAck(String source, Zxid zxid) {
    Message ack = MessageBuilder.buildAck(zxid);
    return new MessageTuple(source, ack);
  }

  private MessageTuple createJoin(String source, Zxid zxid) {
    Message join = MessageBuilder.buildJoin();
    return new MessageTuple(source, join, zxid);
  }

  private MessageTuple createRemove(String serverId, Zxid zxid) {
    Message remove = MessageBuilder.buildRemove(serverId);
    return new MessageTuple(serverId, remove, zxid);
  }

  @Test(timeout=3000)
  public void testAllAck() throws Exception {
    // The cluster contains both server1 and server2, both of them
    // acknowledeged <0, 0>, then <0, 0> should be committed on both sides.
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    TestReceiver receiver1 = new TestReceiver(1);
    TestReceiver receiver2 = new TestReceiver(1);
    NettyTransport transport1 = new NettyTransport(server1, receiver1);
    NettyTransport transport2 = new NettyTransport(server2, receiver2);
    PeerHandler ph1 = new PeerHandler(server1, transport1, 10000);
    PeerHandler ph2 = new PeerHandler(server2, transport2, 10000);
    ph1.startBroadcastingTask();
    ph2.startBroadcastingTask();
    List<String> peers = new ArrayList<String>();
    peers.add(server1);
    peers.add(server2);
    HashMap<String, PeerHandler> quorumSet = new HashMap<String, PeerHandler>();
    quorumSet.put(server1, ph1);
    quorumSet.put(server2, ph2);
    ClusterConfiguration cnf =
      new ClusterConfiguration(Zxid.ZXID_NOT_EXIST, peers, server1);
    AckProcessor ackProcessor =
      new AckProcessor(quorumSet, cnf, Zxid.ZXID_NOT_EXIST);
    Zxid z1 = new Zxid(0, 0);
    ackProcessor.processRequest(createAck(server1, z1));
    ackProcessor.processRequest(createAck(server2, z1));
    // Waits COMMIT is sent to both servers.
    receiver1.latch.await();
    receiver2.latch.await();
  }

  @Test(timeout=3000)
  public void testQuorumAck() throws Exception {
    // The cluster contains three servers, two of them acknowledeged
    // <0, 0>, then <0, 0> should stll be committed on all the servers.
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    TestReceiver receiver1 = new TestReceiver(1);
    TestReceiver receiver2 = new TestReceiver(1);
    TestReceiver receiver3 = new TestReceiver(1);
    NettyTransport transport1 = new NettyTransport(server1, receiver1);
    NettyTransport transport2 = new NettyTransport(server2, receiver2);
    NettyTransport transport3 = new NettyTransport(server3, receiver3);
    PeerHandler ph1 = new PeerHandler(server1, transport1, 10000);
    PeerHandler ph2 = new PeerHandler(server2, transport2, 10000);
    PeerHandler ph3 = new PeerHandler(server3, transport3, 10000);
    ph1.startBroadcastingTask();
    ph2.startBroadcastingTask();
    ph3.startBroadcastingTask();
    List<String> peers = new ArrayList<String>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);
    HashMap<String, PeerHandler> quorumSet = new HashMap<String, PeerHandler>();
    quorumSet.put(server1, ph1);
    quorumSet.put(server2, ph2);
    quorumSet.put(server3, ph3);
    ClusterConfiguration cnf =
      new ClusterConfiguration(Zxid.ZXID_NOT_EXIST, peers, server1);
    AckProcessor ackProcessor =
      new AckProcessor(quorumSet, cnf, Zxid.ZXID_NOT_EXIST);
    Zxid z1 = new Zxid(0, 0);
    ackProcessor.processRequest(createAck(server1, z1));
    ackProcessor.processRequest(createAck(server2, z1));
    // Waits COMMIT is sent to both servers.
    receiver1.latch.await();
    receiver2.latch.await();
    receiver3.latch.await();
  }

  @Test(timeout=3000)
  public void testMinorityAck() throws Exception {
    // The cluster contains three servers, one of them acknowledeged
    // <0, 0>, then <0, 0> shouldn't be committed on all the servers.
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    TestReceiver receiver1 = new TestReceiver(1);
    TestReceiver receiver2 = new TestReceiver(1);
    TestReceiver receiver3 = new TestReceiver(1);
    NettyTransport transport1 = new NettyTransport(server1, receiver1);
    NettyTransport transport2 = new NettyTransport(server2, receiver2);
    NettyTransport transport3 = new NettyTransport(server3, receiver3);
    PeerHandler ph1 = new PeerHandler(server1, transport1, 10000);
    PeerHandler ph2 = new PeerHandler(server2, transport2, 10000);
    PeerHandler ph3 = new PeerHandler(server3, transport3, 10000);
    ph1.startBroadcastingTask();
    ph2.startBroadcastingTask();
    ph3.startBroadcastingTask();
    List<String> peers = new ArrayList<String>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);
    HashMap<String, PeerHandler> quorumSet = new HashMap<String, PeerHandler>();
    quorumSet.put(server1, ph1);
    quorumSet.put(server2, ph2);
    quorumSet.put(server3, ph3);
    ClusterConfiguration cnf =
      new ClusterConfiguration(Zxid.ZXID_NOT_EXIST, peers, server1);
    AckProcessor ackProcessor =
      new AckProcessor(quorumSet, cnf, Zxid.ZXID_NOT_EXIST);
    Zxid z1 = new Zxid(0, 0);
    ackProcessor.processRequest(createAck(server1, z1));
    // The transaction shouldn't been committed.
    Assert.assertFalse(receiver1.latch.await(500, TimeUnit.MILLISECONDS));
    Assert.assertFalse(receiver2.latch.await(10, TimeUnit.MILLISECONDS));
    Assert.assertFalse(receiver3.latch.await(10, TimeUnit.MILLISECONDS));
  }

  @Test(timeout=3000)
  public void testDifferentAck() throws Exception {
    // The cluster contains three servers, server1 acks <0, 2>, server2 acks
    // <0, 1>, server3 acks <0, 0>, then <0, 1> should be committed.
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    TestReceiver receiver1 = new TestReceiver(1);
    TestReceiver receiver2 = new TestReceiver(1);
    TestReceiver receiver3 = new TestReceiver(1);
    NettyTransport transport1 = new NettyTransport(server1, receiver1);
    NettyTransport transport2 = new NettyTransport(server2, receiver2);
    NettyTransport transport3 = new NettyTransport(server3, receiver3);
    PeerHandler ph1 = new PeerHandler(server1, transport1, 10000);
    PeerHandler ph2 = new PeerHandler(server2, transport2, 10000);
    PeerHandler ph3 = new PeerHandler(server3, transport3, 10000);
    ph1.startBroadcastingTask();
    ph2.startBroadcastingTask();
    ph3.startBroadcastingTask();
    List<String> peers = new ArrayList<String>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);
    HashMap<String, PeerHandler> quorumSet = new HashMap<String, PeerHandler>();
    quorumSet.put(server1, ph1);
    quorumSet.put(server2, ph2);
    quorumSet.put(server3, ph3);
    ClusterConfiguration cnf =
      new ClusterConfiguration(Zxid.ZXID_NOT_EXIST, peers, server1);
    AckProcessor ackProcessor =
      new AckProcessor(quorumSet, cnf, Zxid.ZXID_NOT_EXIST);
    Zxid z1 = new Zxid(0, 0);
    ackProcessor.processRequest(createAck(server1, new Zxid(0, 2)));
    ackProcessor.processRequest(createAck(server2, new Zxid(0, 1)));
    ackProcessor.processRequest(createAck(server3, new Zxid(0, 0)));
    // Waits COMMIT is sent to both servers.
    receiver1.latch.await();
    receiver2.latch.await();
    receiver3.latch.await();
    Assert.assertEquals(new Zxid(0, 1), receiver1.committedZxids.get(0));
    Assert.assertEquals(new Zxid(0, 1), receiver2.committedZxids.get(0));
    Assert.assertEquals(new Zxid(0, 1), receiver3.committedZxids.get(0));
  }

  @Test(timeout=3000)
  public void testJoin() throws Exception {
    // Initially, the cluster only contains server1, then server2 joins with
    // COP zxid <0, 2>, then server1 acks <0, 10>, server1 and server2
    // should only get committed zxid <0, 1> since <0, 2> and <0, 3> belong
    // to new configuration of quorum size 2.
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    TestReceiver receiver1 = new TestReceiver(1);
    TestReceiver receiver2 = new TestReceiver(1);
    NettyTransport transport1 = new NettyTransport(server1, receiver1);
    NettyTransport transport2 = new NettyTransport(server2, receiver2);
    PeerHandler ph1 = new PeerHandler(server1, transport1, 10000);
    PeerHandler ph2 = new PeerHandler(server2, transport2, 10000);
    ph1.startBroadcastingTask();
    ph2.startBroadcastingTask();
    List<String> peers = new ArrayList<String>();
    peers.add(server1);
    //peers.add(server2);
    HashMap<String, PeerHandler> quorumSet = new HashMap<String, PeerHandler>();
    quorumSet.put(server1, ph1);
    //quorumSet.put(server2, ph2);
    ClusterConfiguration cnf =
      new ClusterConfiguration(Zxid.ZXID_NOT_EXIST, peers, server1);
    AckProcessor ackProcessor =
      new AckProcessor(quorumSet, cnf, Zxid.ZXID_NOT_EXIST);
    // Update "original" quorumset so the cloned quorumSet in AckProcessor can
    // access this one.
    quorumSet.put(server2, ph2);
    ackProcessor.processRequest(createJoin(server2, new Zxid(0, 2)));
    ackProcessor.processRequest(createAck(server1, new Zxid(0, 10)));
    // Waits COMMIT is sent to both servers.
    receiver1.latch.await();
    receiver2.latch.await();
    // The transactions before COP(<0, 2>) can be committed.
    Assert.assertEquals(new Zxid(0, 1), receiver1.committedZxids.get(0));
    Assert.assertEquals(new Zxid(0, 1), receiver2.committedZxids.get(0));
    // There should be only one committed message.
    Assert.assertEquals(1, receiver1.committedZxids.size());
    Assert.assertEquals(1, receiver2.committedZxids.size());
  }

  @Test(timeout=3000)
  public void testRemove() throws Exception {
    // Initially, the cluster contains two servers, then AckProcessor gets
    // REMOVE message for server2 with Zxid <0, 2>. Later server1 acks <0, 3>,
    // Although <0, 3> has not been acknowledged by quorum of old configuration
    // it will still be committed since it's committed in new configuration.
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    TestReceiver receiver1 = new TestReceiver(1);
    TestReceiver receiver2 = new TestReceiver(1);
    NettyTransport transport1 = new NettyTransport(server1, receiver1);
    NettyTransport transport2 = new NettyTransport(server2, receiver2);
    PeerHandler ph1 = new PeerHandler(server1, transport1, 10000);
    PeerHandler ph2 = new PeerHandler(server2, transport2, 10000);
    ph1.startBroadcastingTask();
    ph2.startBroadcastingTask();
    List<String> peers = new ArrayList<String>();
    peers.add(server1);
    peers.add(server2);
    HashMap<String, PeerHandler> quorumSet = new HashMap<String, PeerHandler>();
    quorumSet.put(server1, ph1);
    quorumSet.put(server2, ph2);
    ClusterConfiguration cnf =
      new ClusterConfiguration(Zxid.ZXID_NOT_EXIST, peers, server1);
    AckProcessor ackProcessor =
      new AckProcessor(quorumSet, cnf, Zxid.ZXID_NOT_EXIST);
    ackProcessor.processRequest(createRemove(server2, new Zxid(0, 2)));
    ackProcessor.processRequest(createAck(server1, new Zxid(0, 3)));
    // Waits COMMIT is sent to both servers.
    receiver1.latch.await();
    receiver2.latch.await();
    // The transactions before COP(<0, 2>) can be committed.
    Assert.assertEquals(new Zxid(0, 3), receiver1.committedZxids.get(0));
    Assert.assertEquals(new Zxid(0, 3), receiver2.committedZxids.get(0));
    // There should be only one committed message.
    Assert.assertEquals(1, receiver1.committedZxids.size());
    Assert.assertEquals(1, receiver2.committedZxids.size());
  }

  @Test(timeout=3000)
  public void testRemoveItself() throws Exception {
    // The cluster has only one server, and the server removes itself from the
    // cluster.
    String server1 = getUniqueHostPort();
    TestReceiver receiver1 = new TestReceiver(1);
    NettyTransport transport1 = new NettyTransport(server1, receiver1);
    PeerHandler ph1 = new PeerHandler(server1, transport1, 10000);
    ph1.startBroadcastingTask();
    List<String> peers = new ArrayList<String>();
    peers.add(server1);
    HashMap<String, PeerHandler> quorumSet = new HashMap<String, PeerHandler>();
    quorumSet.put(server1, ph1);
    ClusterConfiguration cnf =
      new ClusterConfiguration(Zxid.ZXID_NOT_EXIST, peers, server1);
    AckProcessor ackProcessor =
      new AckProcessor(quorumSet, cnf, Zxid.ZXID_NOT_EXIST);
    Zxid z1 = new Zxid(0, 0);
    ackProcessor.processRequest(createRemove(server1, z1));
    ackProcessor.processRequest(createAck(server1, z1));
    // Waits COMMIT is sent to both servers.
    receiver1.latch.await();
  }
}
