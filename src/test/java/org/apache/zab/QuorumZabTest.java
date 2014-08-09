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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.zab.QuorumZab.FailureCaseCallback;
import org.apache.zab.QuorumZab.SimulatedException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for tests.
 */
class QuorumTestCallback implements QuorumZab.StateChangeCallback {

  private static final Logger LOG =
      LoggerFactory.getLogger(QuorumTestCallback.class);

  int establishedEpoch = -1;
  int acknowledgedEpoch = -1;
  String electedLeader;
  String syncFollower;
  Zxid syncZxid;
  int syncAckEpoch;
  List<Transaction> initialHistory = new ArrayList<Transaction>();

  Semaphore semElection = new Semaphore(0);
  Semaphore semDiscovering = new Semaphore(0);
  Semaphore semSynchronizing = new Semaphore(0);
  Semaphore semBroadcasting = new Semaphore(0);
  Semaphore semCopCommit = new Semaphore(0);
  Semaphore semExit = new Semaphore(0);

  ClusterConfiguration clusterConfig;

  void waitElection() throws InterruptedException {
    this.semElection.acquire();
  }

  void waitDiscovering() throws InterruptedException {
    this.semDiscovering.acquire();
  }

  void waitSynchronizing() throws InterruptedException {
    this.semSynchronizing.acquire();
  }

  void waitBroadcasting() throws InterruptedException {
    this.semBroadcasting.acquire();
  }

  void waitExit() throws InterruptedException {
    this.semExit.acquire();
  }

  void waitCopCommit() throws InterruptedException {
    this.semCopCommit.acquire();
  }

  @Override
  public void electing() {
    this.semElection.release();
  }

  @Override
  public void leaderDiscovering(String leader) {
    this.electedLeader = leader;
    this.semDiscovering.release();
  }

  @Override
  public void followerDiscovering(String leader) {
    this.electedLeader = leader;
    this.semDiscovering.release();
  }

  @Override
  public void initialHistoryOwner(String server, int aEpoch, Zxid zxid) {
    this.syncFollower = server;
    this.syncZxid =zxid;
    this.syncAckEpoch =aEpoch;
  }

  @Override
  public void leaderSynchronizing(int epoch) {
    this.establishedEpoch = epoch;
    this.semSynchronizing.release();
  }

  @Override
  public void followerSynchronizing(int epoch) {
    this.establishedEpoch = epoch;
    this.semSynchronizing.release();
  }

  @Override
  public void leaderBroadcasting(int epoch, List<Transaction> history,
                                 ClusterConfiguration config) {
    LOG.debug("The history after synchronization:");
    for (Transaction txn : history) {
      LOG.debug("Txn zxid : {}", txn.getZxid());
    }
    this.acknowledgedEpoch = epoch;
    this.initialHistory = history;
    this.clusterConfig = config;
    this.semBroadcasting.release();
  }

  @Override
  public void followerBroadcasting(int epoch, List<Transaction> history,
                                   ClusterConfiguration config) {
    LOG.debug("The history after synchronization:");
    for (Transaction txn : history) {
      LOG.debug("Txn zxid : {}", txn.getZxid());
    }
    this.acknowledgedEpoch = epoch;
    this.initialHistory = history;
    this.clusterConfig = config;
    this.semBroadcasting.release();
  }

  @Override
  public void leftCluster() {
    this.semExit.release();
  }

  @Override
  public void commitCop() {
    this.semCopCommit.release();
  }
}

/**
 * QuorumZab Test.
 */
public class QuorumZabTest extends TestBase  {

  private static final Logger LOG
    = LoggerFactory.getLogger(QuorumZabTest.class);

  /**
   * Test if the new epoch is established.
   *
   * @throws InterruptedException
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=8000)
  public void testEstablishNewEpoch() throws InterruptedException, IOException {
    QuorumTestCallback cb = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    String servers = server1 + ";" + server2 + ";" + server3;

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                servers,
                                                getDirectory())
                                     .setLog(new DummyLog(10))
                                     .setAckEpoch(0);

    QuorumZab zab1 = new QuorumZab(st, cb, null, state1);

    DummyLog log = new DummyLog(5);

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                servers,
                                                getDirectory())
                                     .setLog(log)
                                     .setProposedEpoch(2)
                                     .setAckEpoch(1);


    QuorumZab zab2 = new QuorumZab(st, null, null, state2);

    QuorumZab.TestState state3 = new QuorumZab
                                     .TestState(server3,
                                                servers,
                                                getDirectory())
                                     .setLog(log)
                                     .setProposedEpoch(2)
                                     .setAckEpoch(1);

    QuorumZab zab3 = new QuorumZab(st, null, null, state3);

    cb.waitBroadcasting();
    // The established epoch should be 3.
    Assert.assertEquals(cb.establishedEpoch, 3);

    // The elected leader should be server1.
    Assert.assertEquals(cb.electedLeader, server1);

    // server 2 and server 3 should have the "best" history.
    Assert.assertTrue(cb.syncFollower.equals(server2) ||
                      cb.syncFollower.equals(server3));

    // The last zxid of the owner of initial history should be (0, 4)
    Assert.assertEquals(cb.syncZxid.compareTo(new Zxid(0, 4)), 0);

    // The last ack epoch of the owner of initial history should be 1.
    Assert.assertEquals(cb.syncAckEpoch, 1);

    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }

  /**
   * Make sure the leader can start up by itself.
   */
  @Test(timeout=8000)
  public void testSingleServer() throws InterruptedException, IOException {
    QuorumTestCallback cb = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();

    QuorumZab.TestState state = new QuorumZab
                                    .TestState(server1,
                                               server1,
                                               getDirectory())
                                    .setLog(new DummyLog(0));

    QuorumZab zab1 = new QuorumZab(st, cb, null, state);

    cb.waitBroadcasting();
    Assert.assertEquals(0, cb.acknowledgedEpoch);
    Assert.assertEquals(0, cb.establishedEpoch);
    Assert.assertEquals(server1, cb.electedLeader);
    Assert.assertEquals(server1, cb.syncFollower);
    Assert.assertTrue(cb.initialHistory.isEmpty());
    Assert.assertEquals(0, cb.syncZxid.compareTo(Zxid.ZXID_NOT_EXIST));

    zab1.shutdown();
  }

  /**
   * Test synchronization case 1.
   *
   * @throws InterruptedException
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=8000)
  public void testSynchronizationCase1()
      throws InterruptedException, IOException {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    String servers = server1 + ";" + server2 + ";" + server3;

    /*
     * Case 1
     *
     * Before Synchronization.
     *
     *  L : <0, 0> (f.a = 0)
     *  F : <0, 0> (f.a = 0)
     *
     * Expected history of Leader and Follower after synchronization:
     *
     * <0, 0>
     */

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                servers,
                                                getDirectory())
                                     .setLog(new DummyLog(1))
                                     .setAckEpoch(0);

    QuorumZab zab1 = new QuorumZab(st, cb1, null, state1);

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                servers,
                                                getDirectory())
                                     .setLog(new DummyLog(1))
                                     .setAckEpoch(0);

    QuorumZab zab2 = new QuorumZab(st, cb2, null, state2);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();

    Assert.assertEquals(cb1.initialHistory.size(), 1);
    Assert.assertEquals(cb1.initialHistory.get(0).getZxid(), new Zxid(0, 0));

    Assert.assertTrue(cb2.initialHistory.size() == 1);
    Assert.assertEquals(cb2.initialHistory.get(0).getZxid(), new Zxid(0, 0));

    zab1.shutdown();
    zab2.shutdown();
  }

  /**
   * Test synchronization case 2.
   *
   * @throws InterruptedException
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=8000)
  public void testSynchronizationCase2()
      throws InterruptedException, IOException {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    String servers = server1 + ";" + server2 + ";" + server3;

    /*
     * Case 2
     *
     * Before Synchronization.
     *
     *  L : <0, 0> <0, 1> (f.a = 0)
     *  F :               (f.a = 0)
     *
     * Expected history of Leader and Follower after synchronization:
     *
     * <0, 0>, <0, 1>
     */

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                servers,
                                                getDirectory())
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(0);

    QuorumZab zab1 = new QuorumZab(st, cb1, null, state1);

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                servers,
                                                getDirectory())
                                     .setLog(new DummyLog(0))
                                     .setAckEpoch(0);

    QuorumZab zab2 = new QuorumZab(st, cb2, null, state2);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();

    Assert.assertEquals(cb1.initialHistory.size(), 2);
    Assert.assertEquals(cb1.initialHistory.get(0).getZxid(), new Zxid(0, 0));
    Assert.assertEquals(cb1.initialHistory.get(1).getZxid(), new Zxid(0, 1));

    Assert.assertTrue(cb2.initialHistory.size() == 2);
    Assert.assertEquals(cb2.initialHistory.get(0).getZxid(), new Zxid(0, 0));
    Assert.assertEquals(cb2.initialHistory.get(1).getZxid(), new Zxid(0, 1));

    zab1.shutdown();
    zab2.shutdown();
  }

  /**
   * Test synchronization case 3.
   *
   * @throws InterruptedException
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=8000)
  public void testSynchronizationCase3()
      throws InterruptedException, IOException {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    String servers = server1 + ";" + server2 + ";" + server3;

    /*
     * Case 3
     *
     * Before Synchronization.
     *
     *  L :               (f.a = 0)
     *  F : <0, 0>, <0,1> (f.a = 0)
     *
     * Expected history of Leader and Follower after synchronization:
     *
     * <0, 0>, <0, 1>
     */

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                servers,
                                                getDirectory())
                                     .setLog(new DummyLog(0))
                                     .setAckEpoch(0);

    QuorumZab zab1 = new QuorumZab(st, cb1, null, state1);


    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                servers,
                                                getDirectory())
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(0);

    QuorumZab zab2 = new QuorumZab(st, cb2, null, state2);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();

    Assert.assertEquals(cb1.initialHistory.size(), 2);
    Assert.assertEquals(cb1.initialHistory.get(0).getZxid(), new Zxid(0, 0));
    Assert.assertEquals(cb1.initialHistory.get(1).getZxid(), new Zxid(0, 1));

    Assert.assertTrue(cb2.initialHistory.size() == 2);
    Assert.assertEquals(cb2.initialHistory.get(0).getZxid(), new Zxid(0, 0));
    Assert.assertEquals(cb2.initialHistory.get(1).getZxid(), new Zxid(0, 1));

    zab1.shutdown();
    zab2.shutdown();
  }

  /**
   * Test synchronization case 4.
   *
   * @throws InterruptedException
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=8000)
  public void testSynchronizationCase4()
      throws InterruptedException, IOException {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    String servers = server1 + ";" + server2 + ";" + server3;

    /*
     * Case 4
     *
     * Before Synchronization.
     *
     *  L : <0, 0>         <1, 0> (f.a = 1)
     *  F : <0, 0>, <0,1>         (f.a = 2)
     *
     * Expected history of Leader and Follower after synchronization:
     *
     * <0, 0>, <0, 1>
     */

    DummyLog log = new DummyLog(1);

    log.append(new Transaction(new Zxid(1, 0),
                               ByteBuffer.wrap("1,0".getBytes())));

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                servers,
                                                getDirectory())
                                     .setLog(log)
                                     .setAckEpoch(1);

    QuorumZab zab1 = new QuorumZab(st, cb1, null, state1);

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                servers,
                                                getDirectory())
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(2);

    QuorumZab zab2 = new QuorumZab(st, cb2, null, state2);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();

    Assert.assertEquals(cb1.initialHistory.size(), 2);
    Assert.assertEquals(cb1.initialHistory.get(0).getZxid(), new Zxid(0, 0));
    Assert.assertEquals(cb1.initialHistory.get(1).getZxid(), new Zxid(0, 1));

    Assert.assertTrue(cb2.initialHistory.size() == 2);
    Assert.assertEquals(cb2.initialHistory.get(0).getZxid(), new Zxid(0, 0));
    Assert.assertEquals(cb2.initialHistory.get(1).getZxid(), new Zxid(0, 1));

    zab1.shutdown();
    zab2.shutdown();
  }

  /**
   * Test synchronization case 5.
   *
   * @throws InterruptedException
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=8000)
  public void testSynchronizationCase5()
      throws InterruptedException, IOException {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    String servers = server1 + ";" + server2 + ";" + server3;

    /*
     * Case 5
     *
     * Before Synchronization.
     *
     *  L : <0, 0>  <0, 1>                (f.a = 0)
     *  F : <0, 0>,         <1,0>         (f.a = 1)
     *
     * Expected history of Leader and Follower after synchronization:
     *
     * <0, 0>, <1, 0>
     */

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                servers,
                                                getDirectory())
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(0);

    QuorumZab zab1 = new QuorumZab(st, cb1, null, state1);

    DummyLog log = new DummyLog(1);
    log.append(new Transaction(new Zxid(1, 0),
                               ByteBuffer.wrap("1,0".getBytes())));

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                servers,
                                                getDirectory())
                                     .setLog(log)
                                     .setAckEpoch(1);

    QuorumZab zab2 = new QuorumZab(st, cb2, null, state2);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();

    Assert.assertEquals(cb1.initialHistory.size(), 2);
    Assert.assertEquals(cb1.initialHistory.get(0).getZxid(), new Zxid(0, 0));
    Assert.assertEquals(cb1.initialHistory.get(1).getZxid(), new Zxid(1, 0));

    Assert.assertTrue(cb2.initialHistory.size() == 2);
    Assert.assertEquals(cb2.initialHistory.get(0).getZxid(), new Zxid(0, 0));
    Assert.assertEquals(cb2.initialHistory.get(1).getZxid(), new Zxid(1, 0));

    zab1.shutdown();
    zab2.shutdown();
  }

  /**
   * Test synchronization case 6.
   *
   * @throws InterruptedException
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=8000)
  public void testSynchronizationCase6()
      throws InterruptedException, IOException {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    String servers = server1 + ";" + server2 + ";" + server3;

    /*
     * Case 6
     *
     * Before Synchronization.
     *
     *  L :               (f.a = 1)
     *  F : <0, 0>, <0,1> (f.a = 0)
     *
     * Expected history of Leader and Follower after synchronization:
     *
     *  <empty>
     */

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                servers,
                                                getDirectory())
                                     .setLog(new DummyLog(0))
                                     .setAckEpoch(1);

    QuorumZab zab1 = new QuorumZab(st, cb1, null, state1);

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                servers,
                                                getDirectory())
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(0);

    QuorumZab zab2 = new QuorumZab(st, cb2, null, state2);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();

    Assert.assertEquals(cb1.initialHistory.size(), 0);
    Assert.assertEquals(cb2.initialHistory.size(), 0);

    zab1.shutdown();
    zab2.shutdown();
  }

  /**
   * Test synchronization case 7.
   *
   * Before the synchronization:
   *
   *  L : <0, 0> <0, 1> <0, 2> (f.a = 0)
   *  F : <0, 0> <0, 1> (f.a = 0)
   *
   * Expected history after the synchronization:
   *
   *  L : <0, 0> <0, 1> <0, 2> (f.a = 0)
   *
   * @throws InterruptedException
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=8000)
  public void testSynchronizationCase7()
      throws InterruptedException, IOException {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    String servers = server1 + ";" + server2 + ";" + server3;

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                servers,
                                                getDirectory())
                                     .setLog(new DummyLog(3))
                                     .setAckEpoch(0);
    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                servers,
                                                getDirectory())
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(0);
    QuorumZab zab1 = new QuorumZab(st, cb1, null, state1);
    QuorumZab zab2 = new QuorumZab(st, cb2, null, state2);
    cb1.waitBroadcasting();
    cb2.waitBroadcasting();
    Assert.assertEquals(3, cb1.initialHistory.size());
    Assert.assertEquals(3, cb2.initialHistory.size());

    zab1.shutdown();
    zab2.shutdown();
  }

  /**
   * Test broadcasting.
   *
   * @throws InterruptedException if it's interrupted.
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=10000)
  public void testBroadcasting()
      throws InterruptedException, IOException {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    String servers = server1 + ";" + server2 + ";" + server3;

    // Expecting 5 delivered transactions.
    TestStateMachine st1 = new TestStateMachine(5);
    TestStateMachine st2 = new TestStateMachine(5);
    TestStateMachine st3 = new TestStateMachine(5);

    /*
     *  Before broadcasting.
     *
     *  L : <0, 0>        (f.a = 0)
     *  F : <0, 0> <0, 1> (f.a = 0)
     *  F : <0, 0> <0, 1> (f.a = 0)
     *
     *  Will broadcast :
     *  <2, 0> <2, 1>, <2, 2>
     *
     *  After broadcasting:
     *  <0, 0> <0, 1> <2, 0> <2, 1> <2, 2>
     */

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                servers,
                                                getDirectory())
                                     .setProposedEpoch(0)
                                     .setLog(new DummyLog(1))
                                     .setAckEpoch(0);

    QuorumZab zab1 = new QuorumZab(st1, cb1, null, state1);

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                servers,
                                                getDirectory())
                                     .setProposedEpoch(1)
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(1);

    QuorumZab zab2 = new QuorumZab(st2, cb2, null, state2);

    QuorumZab.TestState state3 = new QuorumZab
                                     .TestState(server3,
                                                servers,
                                                getDirectory())
                                     .setProposedEpoch(1)
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(1);

    QuorumZab zab3 = new QuorumZab(st3, cb3, null, state3);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();
    cb3.waitBroadcasting();

    zab1.send(ByteBuffer.wrap("HelloWorld1".getBytes()));
    zab1.send(ByteBuffer.wrap("HelloWorld2".getBytes()));
    // This request should be forwarded to server1(leader).
    zab2.send(ByteBuffer.wrap("HelloWorld3".getBytes()));

    st1.txnsCount.await();
    st2.txnsCount.await();
    st3.txnsCount.await();

    Assert.assertEquals(5, st1.deliveredTxns.size());
    Assert.assertEquals(5, st2.deliveredTxns.size());
    Assert.assertEquals(5, st3.deliveredTxns.size());

    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }

  /**
   * Test failure case 1.
   *
   * @throws InterruptedException if it's interrupted.
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=10000)
  public void testFailureCase1()
      throws InterruptedException, IOException {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();
    final String server3 = getUniqueHostPort();
    final String servers = server1 + ";" + server2 + ";" + server3;

    /*
     *  This test simulates that server1 will be crashed after first round
     *  of leader election. The remaining two servers will form a quorum and
     *  finally server1 will join the quorum.
     *
     *  server1 : <0, 0>          (f.a = 1)
     *  server2 : <0, 0>          (f.a = 1)
     *  server3 : <0, 0>  <0, 1>  (f.a = 0)
     *
     *  finally:
     *  server1 : <0, 0>
     *  server2 : <0, 0>
     *  server3 : <0, 0>
     */

    FailureCaseCallback fb1 = new FailureCaseCallback() {
      boolean crashed = false;
      @Override
      public void leaderDiscovering() {
        if (!crashed) {
          crashed = true;
          try {
            // Block for 0 ~ 1 seconds
            Thread.sleep((long)(Math.random() * 1000));
          } catch (InterruptedException e) {
            LOG.error("Interrupted!");
          }
          throw new SimulatedException(String.format("%s crashed "
              + "in broadcasting phase", server1));
        }
      }
    };

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                servers,
                                                getDirectory())
                                     .setProposedEpoch(1)
                                     .setLog(new DummyLog(1))
                                     .setAckEpoch(1);

    QuorumZab zab1 = new QuorumZab(st, cb1, fb1, state1);

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                servers,
                                                getDirectory())
                                     .setProposedEpoch(1)
                                     .setLog(new DummyLog(1))
                                     .setAckEpoch(1);

    QuorumZab zab2 = new QuorumZab(st, cb2, null, state2);

    QuorumZab.TestState state3 = new QuorumZab
                                     .TestState(server3,
                                                servers,
                                                getDirectory())
                                     .setProposedEpoch(0)
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(0);

    QuorumZab zab3 = new QuorumZab(st, cb3, null, state3);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();
    cb3.waitBroadcasting();

    Assert.assertEquals(1, cb1.initialHistory.size());
    Assert.assertEquals(1, cb2.initialHistory.size());
    Assert.assertEquals(1, cb3.initialHistory.size());

    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }

  /**
   * Test failure case 2.
   *
   * @throws InterruptedException if it's interrupted.
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=10000)
  public void testFailureCase2()
      throws InterruptedException, IOException {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();
    final String server3 = getUniqueHostPort();
    final String servers = server1 + ";" + server2 + ";" + server3;

    /*
     *  This test simulates that server2 and server3 will be crashed after
     *  first round of leader election. Finally all servers should find a
     *  common leader and all of them will get synchronized.
     *
     *  server1 : <0, 0>          (f.a = 1)
     *  server2 : <0, 0>          (f.a = 1)
     *  server3 : <0, 0>  <0, 1>  (f.a = 0)
     *
     *  finally:
     *  server1 : <0, 0>
     *  server2 : <0, 0>
     *  server3 : <0, 0>
     */

    FailureCaseCallback fb2 = new FailureCaseCallback() {
      boolean crashed = false;
      @Override
      public void followerDiscovering() {
        if (!crashed) {
          crashed = true;
          try {
            // Block for 0 ~ 1 seconds
            Thread.sleep((long)(Math.random() * 1000));
          } catch (InterruptedException e) {
            LOG.error("Interrupted!");
          }
          throw new SimulatedException(String.format("%s crashed "
              + "in broadcasting phase", server2));
        }
      }
    };

    FailureCaseCallback fb3 = new FailureCaseCallback() {
      boolean crashed = false;
      @Override
      public void followerDiscovering() {
        if (!crashed) {
          crashed = true;
          try {
            // Block for 0 ~ 1 seconds
            Thread.sleep((long)(Math.random() * 1000));
          } catch (InterruptedException e) {
            LOG.error("Interrupted!");
          }
          throw new SimulatedException(String.format("%s crashed "
              + "in broadcasting phase", server3));
        }
      }
    };

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                servers,
                                                getDirectory())
                                     .setProposedEpoch(1)
                                     .setLog(new DummyLog(1))
                                     .setAckEpoch(1);

    QuorumZab zab1 = new QuorumZab(st, cb1, null, state1);

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                servers,
                                                getDirectory())
                                     .setProposedEpoch(1)
                                     .setLog(new DummyLog(1))
                                     .setAckEpoch(1);

    QuorumZab zab2 = new QuorumZab(st, cb2, fb2, state2);

    QuorumZab.TestState state3 = new QuorumZab
                                     .TestState(server3,
                                                servers,
                                                getDirectory())
                                     .setProposedEpoch(0)
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(0);

    QuorumZab zab3 = new QuorumZab(st, cb3, fb3, state3);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();
    cb3.waitBroadcasting();

    Assert.assertEquals(1, cb1.initialHistory.size());
    Assert.assertEquals(1, cb2.initialHistory.size());
    Assert.assertEquals(1, cb3.initialHistory.size());

    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }

  /**
   * Test failure case 3.
   *
   * @throws InterruptedException if it's interrupted.
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=10000)
  public void testFailureCase3()
      throws InterruptedException, IOException {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();
    final String server3 = getUniqueHostPort();
    final String servers = server1 + ";" + server2 + ";" + server3;

    /*
     *  This test simulates that the first leader will be crashed after the
     *  new epoch is established.
     *
     *  server1 : <0, 0>          (f.a = 1)
     *  server2 : <0, 0>          (f.a = 1)
     *  server3 : <0, 0>  <0, 1>  (f.a = 0)
     *
     *  finally:
     *  server1 : <0, 0>
     *  server2 : <0, 0>
     *  server3 : <0, 0>
     */

    FailureCaseCallback fb1 = new FailureCaseCallback() {
      boolean crashed = false;
      @Override
      public void leaderSynchronizing() {
        if (!crashed) {
          crashed = true;
          try {
            // Block for 0 ~ 1 seconds
            Thread.sleep((long)(Math.random() * 1000));
          } catch (InterruptedException e) {
            LOG.error("Interrupted!");
          }
          throw new SimulatedException(String.format("%s crashed "
              + "in broadcasting phase", server1));
        }
      }
    };

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                servers,
                                                getDirectory())
                                     .setProposedEpoch(1)
                                     .setLog(new DummyLog(1))
                                     .setAckEpoch(1);

    QuorumZab zab1 = new QuorumZab(st, cb1, fb1, state1);

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                servers,
                                                getDirectory())
                                     .setProposedEpoch(1)
                                     .setLog(new DummyLog(1))
                                     .setAckEpoch(1);

    QuorumZab zab2 = new QuorumZab(st, cb2, null, state2);

    QuorumZab.TestState state3 = new QuorumZab
                                     .TestState(server3,
                                                servers,
                                                getDirectory())
                                     .setProposedEpoch(0)
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(0);

    QuorumZab zab3 = new QuorumZab(st, cb3, null, state3);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();
    cb3.waitBroadcasting();

    Assert.assertEquals(1, cb1.initialHistory.size());
    Assert.assertEquals(1, cb2.initialHistory.size());
    Assert.assertEquals(1, cb3.initialHistory.size());

    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }

  /**
   * Test failure case 4.
   *
   * @throws InterruptedException if it's interrupted.
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=20000)
  public void testFailureCase4()
      throws InterruptedException, IOException {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();
    final String server3 = getUniqueHostPort();
    final String servers = server1 + ";" + server2 + ";" + server3;

    /*
     *  This test simulates that server2 and server3 will be crashed once they
     *  first time receive NEW_EPOCH. Finally all servers should find a common
     *  leader and all of them will get synchronized.
     *
     *  server1 : <0, 0>          (f.a = 1)
     *  server2 : <0, 0>          (f.a = 1)
     *  server3 : <0, 0>  <0, 1>  (f.a = 0)
     *
     *  finally:
     *  server1 : <0, 0>
     *  server2 : <0, 0>
     *  server3 : <0, 0>
     */

    FailureCaseCallback fb2 = new FailureCaseCallback() {
      boolean crashed = false;
      @Override
      public void followerSynchronizing() {
        if (!crashed) {
          crashed = true;
          try {
            // Block for 0 ~ 1 seconds
            Thread.sleep((long)(Math.random() * 1000));
          } catch (InterruptedException e) {
            LOG.error("Interrupted!");
          }
          throw new SimulatedException(String.format("%s crashed "
              + "in broadcasting phase", server2));
        }
      }
    };

    FailureCaseCallback fb3 = new FailureCaseCallback() {
      boolean crashed = false;
      @Override
      public void followerSynchronizing() {
        if (!crashed) {
          crashed = true;
          try {
            // Block for 0 ~ 1 seconds
            Thread.sleep((long)(Math.random() * 1000));
          } catch (InterruptedException e) {
            LOG.error("Interrupted!");
          }
          throw new SimulatedException(String.format("%s crashed "
              + "in broadcasting phase", server3));
        }
      }
    };

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                servers,
                                                getDirectory())
                                     .setProposedEpoch(1)
                                     .setLog(new DummyLog(1))
                                     .setAckEpoch(1);

    QuorumZab zab1 = new QuorumZab(st, cb1, null, state1);

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                servers,
                                                getDirectory())
                                     .setProposedEpoch(1)
                                     .setLog(new DummyLog(1))
                                     .setAckEpoch(1);

    QuorumZab zab2 = new QuorumZab(st, cb2, fb2, state2);

    QuorumZab.TestState state3 = new QuorumZab
                                     .TestState(server3,
                                                servers,
                                                getDirectory())
                                     .setProposedEpoch(0)
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(0);

    QuorumZab zab3 = new QuorumZab(st, cb3, fb3, state3);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();
    cb3.waitBroadcasting();

    Assert.assertEquals(1, cb1.initialHistory.size());
    Assert.assertEquals(1, cb2.initialHistory.size());
    Assert.assertEquals(1, cb3.initialHistory.size());

    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }

  /**
   * Test failure case 5.
   *
   * @throws InterruptedException if it's interrupted.
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=20000)
  public void testFailureCase5()
      throws InterruptedException, IOException {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();
    final String server3 = getUniqueHostPort();
    final String servers = server1 + ";" + server2 + ";" + server3;

    /*
     *  This test simulates that the first leader will be crashed after the
     *  synchronization is done.
     *
     *  server1 : <0, 0>          (f.a = 1)
     *  server2 : <0, 0>          (f.a = 1)
     *  server3 : <0, 0>  <0, 1>  (f.a = 0)
     *
     *  finally:
     *  server1 : <0, 0>
     *  server2 : <0, 0>
     *  server3 : <0, 0>
     */

    FailureCaseCallback fb1 = new FailureCaseCallback() {
      boolean crashed = false;
      @Override
      public void leaderBroadcasting() {
        if (!crashed) {
          crashed = true;
          try {
            // Block for 0 ~ 1 seconds
            Thread.sleep((long)(Math.random() * 1000));
          } catch (InterruptedException e) {
            LOG.error("Interrupted!");
          }
          throw new SimulatedException(String.format("%s crashed "
              + "in broadcasting phase", server1));
        }
      }
    };

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                servers,
                                                getDirectory())
                                     .setProposedEpoch(1)
                                     .setLog(new DummyLog(1))
                                     .setAckEpoch(1);

    QuorumZab zab1 = new QuorumZab(st, cb1, fb1, state1);

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                servers,
                                                getDirectory())
                                     .setProposedEpoch(1)
                                     .setLog(new DummyLog(1))
                                     .setAckEpoch(1);

    QuorumZab zab2 = new QuorumZab(st, cb2, null, state2);

    QuorumZab.TestState state3 = new QuorumZab
                                     .TestState(server3,
                                                servers,
                                                getDirectory())
                                     .setProposedEpoch(0)
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(0);

    QuorumZab zab3 = new QuorumZab(st, cb3, null, state3);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();
    cb3.waitBroadcasting();

    Assert.assertEquals(1, cb1.initialHistory.size());
    Assert.assertEquals(1, cb2.initialHistory.size());
    Assert.assertEquals(1, cb3.initialHistory.size());

    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }

  @Test(timeout=10000)
  public void testReconfigRecoveryCase1()
      throws IOException, InterruptedException {
    /**
     * Recovery case 1:
     *
     * server 1 : <0, 0>
     * last seen config : {peers : server1,server2,server3, version : 0 0}
     *
     * server 2 : <0, 0>
     * last seen config : {peers : server1,server2,server3, version : 0 0}
     *
     * server 3 : <0, 0>
     * last seen config : {peers : server1,server2,server3, version : 0 0}
     *
     * After recovery :
     *  expected config :
     *    last seen config : {peers : server1,server2,server3, version : 0 0}
     */
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();
    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();
    final String server3 = getUniqueHostPort();
    Zxid version = new Zxid(0, 0);
    ArrayList<String> peers = new ArrayList<String>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);
    ClusterConfiguration cnf1 = new ClusterConfiguration(version, peers,
                                                         server1);
    ClusterConfiguration cnf2 = new ClusterConfiguration(version, peers,
                                                         server2);
    ClusterConfiguration cnf3 = new ClusterConfiguration(version, peers,
                                                         server3);
    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                null,
                                                getDirectory())
                                      .setProposedEpoch(0)
                                      .setLog(new DummyLog(1))
                                      .setAckEpoch(0)
                                      .setClusterConfiguration(cnf1);
    QuorumZab zab1 = new QuorumZab(st, cb1, null, state1);

    QuorumZab.TestState state2 = new QuorumZab
        .TestState(server2,
                   null,
                   getDirectory())
         .setProposedEpoch(0)
         .setLog(new DummyLog(1))
         .setAckEpoch(0)
         .setClusterConfiguration(cnf2);
    QuorumZab zab2 = new QuorumZab(st, cb2, null, state2);

    QuorumZab.TestState state3 = new QuorumZab
        .TestState(server3,
                   null,
                   getDirectory())
         .setProposedEpoch(0)
         .setLog(new DummyLog(1))
         .setAckEpoch(0)
         .setClusterConfiguration(cnf3);
    QuorumZab zab3 = new QuorumZab(st, cb3, null, state3);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();
    cb3.waitBroadcasting();

    Assert.assertEquals(cb1.clusterConfig.getVersion(), cnf1.getVersion());
    Assert.assertEquals(cb2.clusterConfig.getVersion(), cnf2.getVersion());
    Assert.assertEquals(cb3.clusterConfig.getVersion(), cnf3.getVersion());

    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }

  @Test(timeout=10000)
  public void testReconfigRecoveryCase2()
      throws IOException, InterruptedException {
    /**
     * Recovery case 2:
     *
     * server 1 : <0, 0>
     * last seen config : {peers : server1,server2, version : 0 0}
     *
     * server 2 : <0, 0>
     * last seen config : {peers : server1,server2,server3, version : 0 1}
     *
     * server 3 : <0, 0>
     * last seen config : {peers : server1,server2,server3, version : 0 1}
     *
     * After recovery :
     *  expected config :
     *    last seen config : {peers : server1,server2,server3, version : 0 1}
     */
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();
    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();
    final String server3 = getUniqueHostPort();
    Zxid version1 = new Zxid(0, 0);
    Zxid version2 = new Zxid(0, 1);
    ArrayList<String> peers1 = new ArrayList<String>();
    peers1.add(server1);
    peers1.add(server2);
    ArrayList<String> peers2 = new ArrayList<String>();
    peers2.add(server1);
    peers2.add(server2);
    peers2.add(server3);
    ClusterConfiguration cnf1 = new ClusterConfiguration(version1, peers1,
                                                         server1);
    ClusterConfiguration cnf2 = new ClusterConfiguration(version2, peers2,
                                                         server2);
    ClusterConfiguration cnf3 = new ClusterConfiguration(version2, peers2,
                                                         server3);
    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                null,
                                                getDirectory())
                                      .setProposedEpoch(0)
                                      .setLog(new DummyLog(1))
                                      .setAckEpoch(0)
                                      .setClusterConfiguration(cnf1);
    QuorumZab zab1 = new QuorumZab(st, cb1, null, state1);

    QuorumZab.TestState state2 = new QuorumZab
        .TestState(server2,
                   null,
                   getDirectory())
         .setProposedEpoch(0)
         .setLog(new DummyLog(1))
         .setAckEpoch(0)
         .setClusterConfiguration(cnf2);
    QuorumZab zab2 = new QuorumZab(st, cb2, null, state2);

    QuorumZab.TestState state3 = new QuorumZab
        .TestState(server3,
                   null,
                   getDirectory())
         .setProposedEpoch(0)
         .setLog(new DummyLog(1))
         .setAckEpoch(0)
         .setClusterConfiguration(cnf3);
    QuorumZab zab3 = new QuorumZab(st, cb3, null, state3);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();
    cb3.waitBroadcasting();

    Assert.assertEquals(cb1.clusterConfig.getVersion(), cnf2.getVersion());
    Assert.assertEquals(cb2.clusterConfig.getVersion(), cnf2.getVersion());
    Assert.assertEquals(cb3.clusterConfig.getVersion(), cnf2.getVersion());
    // server1 should not be selected as leader.
    Assert.assertNotEquals(cb1.electedLeader, server1);
    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }

  @Test(timeout=10000)
  public void testReconfigRecoveryCase3()
      throws IOException, InterruptedException {
    /**
     * Recovery case 3:
     *
     * server 1 : <0, 0>
     * last seen config : {peers : server1,server2, version : 0 1}
     * f.a = 0
     *
     * server 2 : <0, 0> <1, 0>
     * last seen config : {peers : server1,server2,server3, version : 0 0}
     * f.a = 1
     *
     * server 3 : <0, 0>
     * last seen config : {peers : server1,server2,server3, version : 0 0}
     * f.a = 1
     *
     * After recovery :
     *  expected config :
     *    last seen config : {peers : server1,server2,server3 version : 0 0}
     */
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();
    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();
    final String server3 = getUniqueHostPort();
    Zxid version1 = new Zxid(0, 1);
    Zxid version2 = new Zxid(0, 0);
    ArrayList<String> peers1 = new ArrayList<String>();
    peers1.add(server1);
    peers1.add(server2);
    ArrayList<String> peers2 = new ArrayList<String>();
    peers2.add(server1);
    peers2.add(server2);
    peers2.add(server3);
    ClusterConfiguration cnf1 = new ClusterConfiguration(version1, peers1,
                                                         server1);
    ClusterConfiguration cnf2 = new ClusterConfiguration(version2, peers2,
                                                         server2);
    ClusterConfiguration cnf3 = new ClusterConfiguration(version2, peers2,
                                                         server3);
    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                null,
                                                getDirectory())
                                      .setProposedEpoch(0)
                                      .setLog(new DummyLog(1))
                                      .setAckEpoch(0)
                                      .setClusterConfiguration(cnf1);
    QuorumZab zab1 = new QuorumZab(st, cb1, null, state1);

    QuorumZab.TestState state2 = new QuorumZab
        .TestState(server2,
                   null,
                   getDirectory())
         .setProposedEpoch(1)
         .setLog(new DummyLog(2))
         .setAckEpoch(1)
         .setClusterConfiguration(cnf2);
    QuorumZab zab2 = new QuorumZab(st, cb2, null, state2);

    QuorumZab.TestState state3 = new QuorumZab
        .TestState(server3,
                   null,
                   getDirectory())
         .setProposedEpoch(1)
         .setLog(new DummyLog(1))
         .setAckEpoch(1)
         .setClusterConfiguration(cnf3);
    QuorumZab zab3 = new QuorumZab(st, cb3, null, state3);

    //cb1.waitBroadcasting();
    cb2.waitBroadcasting();
    cb3.waitBroadcasting();

    Assert.assertEquals(cb2.clusterConfig.getVersion(), cnf2.getVersion());
    Assert.assertEquals(cb3.clusterConfig.getVersion(), cnf3.getVersion());
    // After synchronization, size of initial history should equal to 2.
    Assert.assertEquals(2, cb2.initialHistory.size());
    Assert.assertEquals(2, cb3.initialHistory.size());
    // server1 should not be selected as leader.
    Assert.assertNotEquals(cb2.electedLeader, server1);
    Assert.assertNotEquals(cb3.electedLeader, server1);

    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }

  @Test(timeout=10000)
  public void testJoinCase1()
      throws IOException, InterruptedException {
    /**
     * Case 1 :
     *
     * 1. starts server1
     * 2. starts server2 join in server1
     * 3. starts server3 join in server1
     * 4. send request 1
     *
     * Then waits for all servers deliver txn1.
     */
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st1 = new TestStateMachine(1);
    TestStateMachine st2 = new TestStateMachine(1);
    TestStateMachine st3 = new TestStateMachine(1);
    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();
    final String server3 = getUniqueHostPort();

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                null,
                                                getDirectory())
                                     .setJoinPeer(server1);
    QuorumZab zab1 = new QuorumZab(st1, cb1, null, state1);

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                null,
                                                getDirectory())
                                     .setJoinPeer(server1);
    QuorumZab zab2 = new QuorumZab(st2, cb2, null, state2);
    cb1.waitCopCommit();

    QuorumZab.TestState state3 = new QuorumZab
                                     .TestState(server3,
                                                null,
                                                getDirectory())
                                     .setJoinPeer(server1);
    QuorumZab zab3 = new QuorumZab(st3, cb3, null, state3);
    cb1.waitCopCommit();

    zab1.send(ByteBuffer.wrap("req1".getBytes()));
    // Waits for the transaction delivered.
    st1.txnsCount.await();
    st2.txnsCount.await();
    st3.txnsCount.await();

    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }

  @Test(timeout=10000)
  public void testJoinCase2()
      throws IOException, InterruptedException {
    /**
     * Case 2 :
     *
     * 1. starts server1
     * 2. send req1
     * 3. send req3
     * 2. starts server2 join in server1
     * 3. starts server3 join in server2
     * 4. send req3
     *
     * Then waits for all servers deliver txn1, txn2, txn3.
     */
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st1 = new TestStateMachine(3);
    TestStateMachine st2 = new TestStateMachine(3);
    TestStateMachine st3 = new TestStateMachine(3);
    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();
    final String server3 = getUniqueHostPort();

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                null,
                                                getDirectory())
                                     .setJoinPeer(server1);
    QuorumZab zab1 = new QuorumZab(st1, cb1, null, state1);
    cb1.waitBroadcasting();

    zab1.send(ByteBuffer.wrap("req1".getBytes()));
    zab1.send(ByteBuffer.wrap("req2".getBytes()));

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                null,
                                                getDirectory())
                                     .setJoinPeer(server1);
    QuorumZab zab2 = new QuorumZab(st2, cb2, null, state2);
    cb1.waitCopCommit();

    QuorumZab.TestState state3 = new QuorumZab
                                     .TestState(server3,
                                                null,
                                                getDirectory())
                                     .setJoinPeer(server2);
    QuorumZab zab3 = new QuorumZab(st3, cb3, null, state3);
    cb3.waitBroadcasting();

    zab1.send(ByteBuffer.wrap("req3".getBytes()));
    // Waits for all the transactions delivered.
    st1.txnsCount.await();
    st2.txnsCount.await();
    st3.txnsCount.await();

    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }

  @Test(timeout=10000)
  public void testJoinCase3()
      throws IOException, InterruptedException {
    /**
     * This test case shows that after reconfiguration is done, the quorum size
     * should be changed.
     *
     * Case 3 :
     *
     * 1. starts server1
     * 2. sends req1
     * 3. starts server2
     * 4. after server2 joins in cluster, it dies.
     * 5. sends req2
     *
     * expected result :
     * txn1 should be delivered on both server1 and server2, since after
     * reconfiguration, the quorum size is changed to 2, the txn2 should
     * never be delivered.
     */
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st1 = new TestStateMachine(2);
    TestStateMachine st2 = new TestStateMachine(1);
    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                null,
                                                getDirectory())
                                     .setJoinPeer(server1);
    QuorumZab zab1 = new QuorumZab(st1, cb1, null, state1);
    cb1.waitBroadcasting();

    zab1.send(ByteBuffer.wrap("req1".getBytes()));
    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                null,
                                                getDirectory())
                                     .setJoinPeer(server1);
    QuorumZab zab2 = new QuorumZab(st2, cb2, null, state2);

    cb2.waitBroadcasting();
    // Simulate server2 dies.
    zab2.shutdown();
    zab1.send(ByteBuffer.wrap("req2".getBytes()));
    // Waits the both txn1 and txn2 to be delivered or timeout on server1.
    boolean isCountedDown = st1.txnsCount.await(500, TimeUnit.MILLISECONDS);
    // It should not be delivered.
    Assert.assertFalse(isCountedDown);
    // Waits for txn1 to be delivered on server2.
    st2.txnsCount.await();
    zab1.shutdown();
  }

  @Test(timeout=10000)
  public void testJoinCase4()
      throws IOException, InterruptedException {
    /**
     * It's as same as testJoinCase3 except server2 gets restarted. So the txn1
     * and txn2 will be eventually delivered on both servers.
     */
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st1 = new TestStateMachine(2);
    TestStateMachine st2 = new TestStateMachine(2);
    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                null,
                                                getDirectory())
                                     .setJoinPeer(server1);
    QuorumZab zab1 = new QuorumZab(st1, cb1, null, state1);
    cb1.waitBroadcasting();
    zab1.send(ByteBuffer.wrap("req1".getBytes()));

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                null,
                                                getDirectory())
                                     .setJoinPeer(server1);
    QuorumZab zab2 = new QuorumZab(st2, cb2, null, state2);
    cb2.waitBroadcasting();

    // Simulate server2 dies.
    zab2.shutdown();
    zab1.send(ByteBuffer.wrap("req2".getBytes()));
    // Waits the both txn1 and txn2 to be delivered or timeout on server1.
    boolean isCountedDown = st1.txnsCount.await(500, TimeUnit.MILLISECONDS);
    // It should not be delivered.
    Assert.assertFalse(isCountedDown);

    // Restart server2.
    state2 = new QuorumZab
                 .TestState(server2,
                            null,
                            getDirectory());
    zab2 = new QuorumZab(st2, cb2, null, state2);
    // Now all the transactions should be delivered.
    st1.txnsCount.await();
    st2.txnsCount.await();
    zab1.shutdown();
  }

  @Test(timeout=10000)
  public void testLeaveCase1()
      throws IOException, InterruptedException {
    /**
     * Case 1 :
     * Follower is removed.
     *
     * 1. starts server1
     * 2. starts server2 join in server1
     * 3. send req1 to server1.
     * 3. server2 leaves cluster.
     * 4. send req2 to server1.
     *
     * server1 delivers both txn1 and txn2. server2 only delivers txn1.
     */
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st1 = new TestStateMachine(2);
    TestStateMachine st2 = new TestStateMachine(1);
    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                null,
                                                getDirectory())
                                     .setJoinPeer(server1);
    QuorumZab zab1 = new QuorumZab(st1, cb1, null, state1);
    cb1.waitBroadcasting();

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                null,
                                                getDirectory())
                                     .setJoinPeer(server1);
    QuorumZab zab2 = new QuorumZab(st2, cb2, null, state2);
    cb1.waitCopCommit();
    zab1.send(ByteBuffer.wrap("req1".getBytes()));
    st2.txnsCount.await();
    zab2.leave();
    // Waits server2's leaving commits.
    cb1.waitCopCommit();
    zab2.shutdown();
    zab1.send(ByteBuffer.wrap("req2".getBytes()));
    // Waits for the transaction delivered.
    st1.txnsCount.await();
    zab1.shutdown();
  }

  @Test(timeout=10000)
  public void testLeaveCase2()
      throws IOException, InterruptedException {
    /**
     * Case 1 :
     * Leader is removed.
     *
     * 1. starts server1
     * 2. starts server2 join in server1
     * 3. server1 leaves.
     *
     */
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st1 = new TestStateMachine(2);
    TestStateMachine st2 = new TestStateMachine(1);
    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState(server1,
                                                null,
                                                getDirectory())
                                     .setJoinPeer(server1);
    QuorumZab zab1 = new QuorumZab(st1, cb1, null, state1);
    cb1.waitBroadcasting();

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState(server2,
                                                null,
                                                getDirectory())
                                     .setJoinPeer(server1);
    QuorumZab zab2 = new QuorumZab(st2, cb2, null, state2);
    cb1.waitCopCommit();
    cb2.waitBroadcasting();
    // Leader leaves current configuration.
    zab1.leave();
    // Waits for server2 goes back to broadcasting phase again.
    cb2.waitBroadcasting();
    zab1.shutdown();
    zab2.shutdown();
  }
}
