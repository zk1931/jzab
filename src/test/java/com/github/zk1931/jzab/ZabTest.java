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

import com.github.zk1931.jzab.Zab.FailureCaseCallback;
import com.github.zk1931.jzab.Zab.SimulatedException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for tests.
 */
class QuorumTestCallback implements Zab.StateChangeCallback {

  private static final Logger LOG =
      LoggerFactory.getLogger(QuorumTestCallback.class);

  long establishedEpoch = -1;
  long acknowledgedEpoch = -1;
  String electedLeader;
  String syncFollower;
  Zxid syncZxid;
  long syncAckEpoch;
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
  public void initialHistoryOwner(String server, long aEpoch, Zxid zxid) {
    this.syncFollower = server;
    this.syncZxid =zxid;
    this.syncAckEpoch =aEpoch;
  }

  @Override
  public void leaderSynchronizing(long epoch) {
    this.establishedEpoch = epoch;
    this.semSynchronizing.release();
  }

  @Override
  public void followerSynchronizing(long epoch) {
    this.establishedEpoch = epoch;
    this.semSynchronizing.release();
  }

  @Override
  public void leaderBroadcasting(long epoch, List<Transaction> history,
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
  public void followerBroadcasting(long epoch, List<Transaction> history,
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
 * Zab Test.
 */
public class ZabTest extends TestBase  {

  private static final Logger LOG
    = LoggerFactory.getLogger(ZabTest.class);

  /**
   * Make sure the leader can start up by itself.
   */
  @Test(timeout=20000)
  public void testSingleServer() throws Exception {
    QuorumTestCallback cb = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    Set<String> peers = new HashSet<>();
    peers.add(server1);

    ZabConfig config = new ZabConfig();
    PersistentState state = makeInitialState(server1, 10);
    Zab zab1 = new Zab(st, config, server1, peers, state, cb, null);

    cb.waitBroadcasting();
    Assert.assertEquals(0, cb.acknowledgedEpoch);
    Assert.assertEquals(0, cb.establishedEpoch);
    Assert.assertEquals(server1, cb.electedLeader);
    Assert.assertEquals(server1, cb.syncFollower);
    zab1.shutdown();
  }

  /**
   * Test synchronization case 1.
   *
   * @throws InterruptedException
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=20000)
  public void testSynchronizationCase1() throws Exception {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);

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

    PersistentState state1 = makeInitialState(server1, 1);
    state1.setAckEpoch(0);

    PersistentState state2 = makeInitialState(server2, 1);
    state2.setAckEpoch(0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st, config1, server1, peers, state1, cb1, null);
    Zab zab2 = new Zab(st, config2, server2, peers, state2, cb2, null);

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
  @Test(timeout=20000)
  public void testSynchronizationCase2() throws Exception {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);

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

    PersistentState state1 = makeInitialState(server1, 2);
    state1.setAckEpoch(0);

    PersistentState state2 = makeInitialState(server2, 0);
    state2.setAckEpoch(0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st, config1, server1, peers, state1, cb1, null);
    Zab zab2 = new Zab(st, config2, server2, peers, state2, cb2, null);

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
  @Test(timeout=20000)
  public void testSynchronizationCase3() throws Exception {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);

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

    PersistentState state1 = makeInitialState(server1, 0);
    state1.setAckEpoch(0);

    PersistentState state2 = makeInitialState(server2, 2);
    state2.setAckEpoch(0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st, config1, server1, peers, state1, cb1, null);
    Zab zab2 = new Zab(st, config2, server2, peers, state2, cb2, null);

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
  @Test(timeout=20000)
  public void testSynchronizationCase4() throws Exception {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);

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

    PersistentState state1 = makeInitialState(server1, 1);
    state1.log.append(new Transaction(new Zxid(1, 0),
                                      ByteBuffer.wrap("1,0".getBytes())));
    state1.setAckEpoch(1);

    PersistentState state2 = makeInitialState(server2, 2);
    state2.setAckEpoch(2);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st, config1, server1, peers, state1, cb1, null);
    Zab zab2 = new Zab(st, config2, server2, peers, state2, cb2, null);

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
  @Test(timeout=20000)
  public void testSynchronizationCase5() throws Exception {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);

    /*
    * Case 5
    *
    * Before Synchronization.
    *
    *  F : <0, 0>  <0, 1>                (f.a = 0)
    *  L : <0, 0>,         <1,0>         (f.a = 1)
    *
    * Expected history of Leader and Follower after synchronization:
    *
    * <0, 0>, <1, 0>
    */

    PersistentState state1 = makeInitialState(server1, 2);
    state1.setAckEpoch(0);

    PersistentState state2 = makeInitialState(server2, 1);
    state2.log.append(new Transaction(new Zxid(1, 0),
                                     ByteBuffer.wrap("1,0".getBytes())));
    state2.setAckEpoch(1);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st, config1, server1, peers, state1, cb1, null);
    Zab zab2 = new Zab(st, config2, server2, peers, state2, cb2, null);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();

    Assert.assertEquals(2, cb1.initialHistory.size());
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
  @Test(timeout=20000)
  public void testSynchronizationCase6() throws Exception {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);

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

    PersistentState state1 = makeInitialState(server1, 0);
    state1.setAckEpoch(1);

    PersistentState state2 = makeInitialState(server2, 2);
    state2.setAckEpoch(0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st, config1, server1, peers, state1, cb1, null);
    Zab zab2 = new Zab(st, config2, server2, peers, state2, cb2, null);

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
  @Test(timeout=20000)
  public void testSynchronizationCase7() throws Exception {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);

    PersistentState state1 = makeInitialState(server1, 3);
    state1.setAckEpoch(0);

    PersistentState state2 = makeInitialState(server2, 2);
    state2.setAckEpoch(0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st, config1, server1, peers, state1, cb1, null);
    Zab zab2 = new Zab(st, config2, server2, peers, state2, cb2, null);

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
  @Test(timeout=20000)
  public void testBroadcasting() throws Exception {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);

    // Expecting 5 delivered transactions.
    TestStateMachine st1 = new TestStateMachine(5);
    TestStateMachine st2 = new TestStateMachine(5);
    TestStateMachine st3 = new TestStateMachine(5);

    /*
     *  Before broadcasting.
     *
     *  L : <0, 0>        (f.a = 0)
     *  F : <0, 0> <0, 1> (f.a = 1)
     *  F : <0, 0> <0, 1> (f.a = 1)
     *
     *  Will broadcast :
     *  <2, 0> <2, 1>, <2, 2>
     *
     *  After broadcasting:
     *  <0, 0> <0, 1> <2, 0> <2, 1> <2, 2>
     */

    PersistentState state1 = makeInitialState(server1, 1);
    state1.setProposedEpoch(0);
    state1.setAckEpoch(0);

    PersistentState state2 = makeInitialState(server2, 2);
    state2.setAckEpoch(1);
    state2.setProposedEpoch(1);

    PersistentState state3 = makeInitialState(server3, 2);
    state3.setAckEpoch(1);
    state3.setProposedEpoch(1);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();
    ZabConfig config3 = new ZabConfig();

    Zab zab1 = new Zab(st1, config1, server1, peers, state1, cb1, null);
    Zab zab2 = new Zab(st2, config2, server2, peers, state2, cb2, null);
    Zab zab3 = new Zab(st3, config3, server3, peers, state3, cb3, null);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();
    cb3.waitBroadcasting();

    zab1.send(ByteBuffer.wrap("HelloWorld1".getBytes()), null);
    zab1.send(ByteBuffer.wrap("HelloWorld2".getBytes()), null);
    zab2.send(ByteBuffer.wrap("HelloWorld3".getBytes()), null);

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
  @Test(timeout=20000)
  public void testFailureCase1() throws Exception {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();
    final String server3 = getUniqueHostPort();
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);

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

    PersistentState state1 = makeInitialState(server1, 1);
    state1.setProposedEpoch(1);
    state1.setAckEpoch(1);

    PersistentState state2 = makeInitialState(server2, 1);
    state2.setAckEpoch(1);
    state2.setProposedEpoch(1);

    PersistentState state3 = makeInitialState(server3, 2);
    state3.setAckEpoch(0);
    state3.setProposedEpoch(0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();
    ZabConfig config3 = new ZabConfig();

    FailureCaseCallback fcb = new FailureCaseCallback() {
      boolean crashed = false;
      @Override
      public void leaderDiscovering() {
        if (!crashed) {
          crashed = true;
          try {
            // Block for 0 ~ 1 seconds
            Thread.sleep((long)(Math.random() * 500));
          } catch (InterruptedException e) {
            LOG.error("Interrupted!");
          }
          throw new SimulatedException(String.format("%s crashed "
              + "in broadcasting phase", server1));
        }
      }
    };

    Zab zab1 = new Zab(st, config1, server1, peers, state1, cb1, fcb);
    Zab zab2 = new Zab(st, config2, server2, peers, state2, cb2, fcb);
    Zab zab3 = new Zab(st, config3, server3, peers, state3, cb3, fcb);

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
  @Test(timeout=20000)
  public void testFailureCase2() throws Exception {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();
    final String server3 = getUniqueHostPort();
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);

    /*
     *  This test simulates that 2 followers will crash after
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

    PersistentState state1 = makeInitialState(server1, 1);
    state1.setProposedEpoch(1);
    state1.setAckEpoch(1);

    PersistentState state2 = makeInitialState(server2, 1);
    state2.setAckEpoch(1);
    state2.setProposedEpoch(1);

    PersistentState state3 = makeInitialState(server3, 2);
    state3.setAckEpoch(0);
    state3.setProposedEpoch(0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();
    ZabConfig config3 = new ZabConfig();

    FailureCaseCallback fcb = new FailureCaseCallback() {
      volatile int crashedCount = 0;
      @Override
      public void followerDiscovering() {
        if (crashedCount < 2) {
          ++crashedCount;
          try {
            // Block for 0 ~ 1 seconds
            Thread.sleep((long)(Math.random() * 500));
          } catch (InterruptedException e) {
            LOG.error("Interrupted!");
          }
          throw new SimulatedException(String.format("%s crashed "
              + "in broadcasting phase", server2));
        }
      }
    };

    Zab zab1 = new Zab(st, config1, server1, peers, state1, cb1, fcb);
    Zab zab2 = new Zab(st, config2, server2, peers, state2, cb2, fcb);
    Zab zab3 = new Zab(st, config3, server3, peers, state3, cb3, fcb);

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
  @Test(timeout=20000)
  public void testFailureCase3() throws Exception {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();
    final String server3 = getUniqueHostPort();
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);

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

    PersistentState state1 = makeInitialState(server1, 1);
    state1.setProposedEpoch(1);
    state1.setAckEpoch(1);

    PersistentState state2 = makeInitialState(server2, 1);
    state2.setAckEpoch(1);
    state2.setProposedEpoch(1);

    PersistentState state3 = makeInitialState(server3, 2);
    state3.setAckEpoch(0);
    state3.setProposedEpoch(0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();
    ZabConfig config3 = new ZabConfig();

    FailureCaseCallback fcb = new FailureCaseCallback() {
      boolean crashed = false;
      @Override
      public void leaderSynchronizing() {
        if (!crashed) {
          crashed = true;
          try {
            // Block for 0 ~ 1 seconds
            Thread.sleep((long)(Math.random() * 500));
          } catch (InterruptedException e) {
            LOG.error("Interrupted!");
          }
          throw new SimulatedException(String.format("%s crashed "
              + "in broadcasting phase", server1));
        }
      }
    };

    Zab zab1 = new Zab(st, config1, server1, peers, state1, cb1, fcb);
    Zab zab2 = new Zab(st, config2, server2, peers, state2, cb2, fcb);
    Zab zab3 = new Zab(st, config3, server3, peers, state3, cb3, fcb);

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
  public void testFailureCase4() throws Exception {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();
    final String server3 = getUniqueHostPort();
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);

    /*
     *  This test simulates that two followers will be crash once they
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

    PersistentState state1 = makeInitialState(server1, 1);
    state1.setProposedEpoch(1);
    state1.setAckEpoch(1);

    PersistentState state2 = makeInitialState(server2, 1);
    state2.setAckEpoch(1);
    state2.setProposedEpoch(1);

    PersistentState state3 = makeInitialState(server3, 2);
    state3.setAckEpoch(0);
    state3.setProposedEpoch(0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();
    ZabConfig config3 = new ZabConfig();

    FailureCaseCallback fcb = new FailureCaseCallback() {
      volatile int crashCount = 0;
      @Override
      public void followerSynchronizing() {
        if (crashCount < 2) {
          ++crashCount;
          try {
            // Block for 0 ~ 1 seconds
            Thread.sleep((long)(Math.random() * 500));
          } catch (InterruptedException e) {
            LOG.error("Interrupted!");
          }
          throw new SimulatedException(String.format("%s crashed "
              + "in broadcasting phase", server2));
        }
      }
    };

    Zab zab1 = new Zab(st, config1, server1, peers, state1, cb1, fcb);
    Zab zab2 = new Zab(st, config2, server2, peers, state2, cb2, fcb);
    Zab zab3 = new Zab(st, config3, server3, peers, state3, cb3, fcb);

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
  public void testFailureCase5() throws Exception {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();
    final String server3 = getUniqueHostPort();
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);

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

    PersistentState state1 = makeInitialState(server1, 1);
    state1.setProposedEpoch(1);
    state1.setAckEpoch(1);

    PersistentState state2 = makeInitialState(server2, 1);
    state2.setAckEpoch(1);
    state2.setProposedEpoch(1);

    PersistentState state3 = makeInitialState(server3, 2);
    state3.setAckEpoch(0);
    state3.setProposedEpoch(0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();
    ZabConfig config3 = new ZabConfig();

    FailureCaseCallback fcb = new FailureCaseCallback() {
      boolean crashed = false;
      @Override
      public void leaderBroadcasting() {
        if (!crashed) {
          crashed = true;
          try {
            // Block for 0 ~ 1 seconds
            Thread.sleep((long)(Math.random() * 500));
          } catch (InterruptedException e) {
            LOG.error("Interrupted!");
          }
          throw new SimulatedException(String.format("%s crashed "
              + "in broadcasting phase", server1));
        }
      }
    };

    Zab zab1 = new Zab(st, config1, server1, peers, state1, cb1, fcb);
    Zab zab2 = new Zab(st, config2, server2, peers, state2, cb2, fcb);
    Zab zab3 = new Zab(st, config3, server3, peers, state3, cb3, fcb);

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

  @Test(timeout=20000)
  public void testReconfigRecoveryCase1() throws Exception {
    /*
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

    PersistentState state1 = makeInitialState(server1, 1);
    state1.setProposedEpoch(0);
    state1.setAckEpoch(0);
    state1.setLastSeenConfig(cnf1);

    PersistentState state2 = makeInitialState(server2, 1);
    state2.setAckEpoch(0);
    state2.setProposedEpoch(0);
    state2.setLastSeenConfig(cnf2);

    PersistentState state3 = makeInitialState(server3, 1);
    state3.setAckEpoch(0);
    state3.setProposedEpoch(0);
    state3.setLastSeenConfig(cnf3);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();
    ZabConfig config3 = new ZabConfig();

    Zab zab1 = new Zab(st, config1, state1, cb1, null);
    Zab zab2 = new Zab(st, config2, state2, cb2, null);
    Zab zab3 = new Zab(st, config3, state3, cb3, null);

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

  @Test(timeout=20000)
  public void testReconfigRecoveryCase2() throws Exception {
    /*
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

    PersistentState state1 = makeInitialState(server1, 1);
    state1.setProposedEpoch(0);
    state1.setAckEpoch(0);
    state1.setLastSeenConfig(cnf1);

    PersistentState state2 = makeInitialState(server2, 2);
    state2.setAckEpoch(0);
    state2.setProposedEpoch(0);
    state2.setLastSeenConfig(cnf2);

    PersistentState state3 = makeInitialState(server3, 2);
    state3.setAckEpoch(0);
    state3.setProposedEpoch(0);
    state3.setLastSeenConfig(cnf3);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();
    ZabConfig config3 = new ZabConfig();

    Zab zab1 = new Zab(st, config1, state1, cb1, null);
    Zab zab2 = new Zab(st, config2, state2, cb2, null);
    Zab zab3 = new Zab(st, config3, state3, cb3, null);

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

  @Test(timeout=20000)
  public void testReconfigRecoveryCase3() throws Exception {
    /*
     * Recovery case 3:
     *
     * server 1 : <0, 0>
     * last seen config : {peers : server1,server2, version : 0 1}
     * f.a = 0
     *
     * server 2 : <0, 0> <0, 1>
     * last seen config : {peers : server1,server2,server3, version : 0 0}
     * f.a = 1
     *
     * server 3 : <0, 0> <0, 1>
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

    PersistentState state1 = makeInitialState(server1, 2);
    state1.setProposedEpoch(0);
    state1.setAckEpoch(0);
    state1.setLastSeenConfig(cnf1);

    PersistentState state2 = makeInitialState(server2, 2);
    state2.setAckEpoch(1);
    state2.setProposedEpoch(1);
    state2.setLastSeenConfig(cnf2);

    PersistentState state3 = makeInitialState(server3, 2);
    state3.setAckEpoch(1);
    state3.setProposedEpoch(1);
    state3.setLastSeenConfig(cnf3);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();
    ZabConfig config3 = new ZabConfig();

    Zab zab1 = new Zab(st, config1, state1, cb1, null);
    Zab zab2 = new Zab(st, config2, state2, cb2, null);
    Zab zab3 = new Zab(st, config3, state3, cb3, null);

    cb1.waitBroadcasting();
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

  @Test(timeout=20000)
  public void testJoinCase1() throws Exception {
    /*
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

    PersistentState state1 = makeInitialState(server1, 0);
    PersistentState state2 = makeInitialState(server2, 0);
    PersistentState state3 = makeInitialState(server3, 0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();
    ZabConfig config3 = new ZabConfig();

    Zab zab1 = new Zab(st1, config1, server1, server1, state1, cb1, null);
    cb1.waitBroadcasting();

    Zab zab2 = new Zab(st2, config2, server2, server1, state2, cb2, null);
    cb1.waitCopCommit();

    Zab zab3 = new Zab(st3, config3, server3, server1, state3, cb3, null);
    cb1.waitCopCommit();

    zab1.send(ByteBuffer.wrap("req1".getBytes()), null);
    // Waits for the transaction delivered.
    st1.txnsCount.await();
    st2.txnsCount.await();
    st3.txnsCount.await();

    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }

  @Test(timeout=20000)
  public void testJoinCase2() throws Exception {
    /*
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

    PersistentState state1 = makeInitialState(server1, 0);
    PersistentState state2 = makeInitialState(server2, 0);
    PersistentState state3 = makeInitialState(server3, 0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();
    ZabConfig config3 = new ZabConfig();

    Zab zab1 = new Zab(st1, config1, server1, server1, state1, cb1, null);
    cb1.waitBroadcasting();
    zab1.send(ByteBuffer.wrap("req1".getBytes()), null);
    zab1.send(ByteBuffer.wrap("req2".getBytes()), null);

    Zab zab2 = new Zab(st2, config2, server2, server1, state2, cb2, null);
    cb1.waitCopCommit();

    Zab zab3 = new Zab(st3, config3, server3, server1, state3, cb3, null);
    cb1.waitCopCommit();
    cb3.waitBroadcasting();

    zab1.send(ByteBuffer.wrap("req3".getBytes()), null);

    // Waits for all the transactions delivered.
    st1.txnsCount.await();
    st2.txnsCount.await();
    st3.txnsCount.await();

    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }

  @Test(timeout=20000)
  public void testJoinCase3() throws Exception {
    /*
     * This test case shows that after reconfiguration is done, the quorum
     * size should be changed.
     *
     * Case 3 :
     *
     * 1. starts server1
     * 2. sends req1
     * 3. starts server2
     * 4. after server2 joins in cluster, it dies.
     * 5. sends req2
     *
     * Since we lose a quorum, req2 will never be delivered.
     */
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st1 = new TestStateMachine(2);
    TestStateMachine st2 = new TestStateMachine(1);

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();

    PersistentState state1 = makeInitialState(server1, 0);
    PersistentState state2 = makeInitialState(server2, 0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st1, config1, server1, server1, state1, cb1, null);
    cb1.waitBroadcasting();
    zab1.send(ByteBuffer.wrap("req1".getBytes()), null);

    Zab zab2 = new Zab(st2, config2, server2, server1, state2, cb2, null);
    cb2.waitBroadcasting();
    st2.txnsCount.await();

    // Simulate server2 dies.
    zab2.shutdown();

    try {
      zab1.send(ByteBuffer.wrap("req2".getBytes()), null);
    } catch (ZabException.InvalidPhase ex) {
      LOG.debug("No in broadcasting phase.");
    }
    // Waits the both txn1 and txn2 to be delivered or timeout on server1.
    boolean isCountedDown = st1.txnsCount.await(500, TimeUnit.MILLISECONDS);
    // It should not be delivered.
    Assert.assertFalse(isCountedDown);
    // Waits for txn1 to be delivered on server2.
    st2.txnsCount.await();
    zab1.shutdown();
  }

  @Test(timeout=20000)
  public void testRemoveCase1() throws Exception {
    /*
     * Case 1 :
     * Follower is removed.
     *
     * 1. starts server1
     * 2. starts server2 join in server1
     * 3. send req1 to server1.
     * 3. server1 removes server2.
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

    PersistentState state1 = makeInitialState(server1, 0);
    PersistentState state2 = makeInitialState(server2, 0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st1, config1, server1, server1, state1, cb1, null);
    cb1.waitBroadcasting();

    Zab zab2 = new Zab(st2, config2, server2, server1, state2, cb2, null);
    cb2.waitBroadcasting();

    zab1.send(ByteBuffer.wrap("req1".getBytes()), null);
    zab1.remove(server2, null);
    // Waits server2 exits.
    cb2.waitExit();
    zab1.send(ByteBuffer.wrap("req2".getBytes()), null);
    st1.txnsCount.await();
    st2.txnsCount.await();
    zab1.shutdown();
    zab2.shutdown();
  }

  @Test(timeout=20000)
  public void testRemoveCase2() throws Exception {
    /*
     * Case 2 :
     * Follower removes the leader from the cluster.
     *
     * 1. starts server1
     * 2. starts server2 join in server1
     * 3. server2 removes server1.
     *
     */
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st1 = new TestStateMachine(2);
    TestStateMachine st2 = new TestStateMachine(1);

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();

    PersistentState state1 = makeInitialState(server1, 0);
    PersistentState state2 = makeInitialState(server2, 0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st1, config1, server1, server1, state1, cb1, null);
    cb1.waitBroadcasting();

    Zab zab2 = new Zab(st2, config2, server2, server1, state2, cb2, null);
    cb2.waitBroadcasting();

    // serve2 removes server1.
    zab2.remove(server1, null);
    // Waits old leader exits.
    cb1.waitExit();
    // Waits for server2 goes back to broadcasting phase again.
    cb2.waitBroadcasting();
    zab1.shutdown();
    zab2.shutdown();
  }

  @Test(timeout=20000)
  public void testCallback() throws Exception {
    // Tests the clients' callback.
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st1 = new TestStateMachine(2);
    TestStateMachine st2 = new TestStateMachine(2);

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();

    PersistentState state1 = makeInitialState(server1, 0);
    PersistentState state2 = makeInitialState(server2, 0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st1, config1, server1, server1, state1, cb1, null);
    // Waits for clusterChange and leading callback.
    st1.waitMembershipChanged();
    // Make sure first config contains itself.
    Assert.assertTrue(st1.clusters.contains(server1));
    // The cluster size should be 1.
    Assert.assertEquals(1, st1.clusters.size());

    Zab zab2 = new Zab(st2, config2, server2, server1, state2, cb2, null);
    st2.waitMembershipChanged();
    // Make sure first config contains itself.
    Assert.assertTrue(st2.clusters.contains(server1));
    // The cluster size should be 2.
    Assert.assertEquals(2, st2.clusters.size());
    // Now the callback will be called again.
    st1.waitMembershipChanged();
    // Make sure first config contains itself.
    Assert.assertTrue(st1.clusters.contains(server1));
    // The cluster size should be 1.
    Assert.assertEquals(2, st1.clusters.size());
    zab1.shutdown();
    zab2.shutdown();
  }

  @Test(timeout=20000)
  public void testSendingWhileJoining() throws Exception {
    // Tests keep sending request while someone joins in, in the end two
    // servers should have the same state.
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st1 = new TestStateMachine(100);
    TestStateMachine st2 = new TestStateMachine(100);

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();

    PersistentState state1 = makeInitialState(server1, 0);
    PersistentState state2 = makeInitialState(server2, 0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st1, config1, server1, server1, state1, cb1, null);
    cb1.waitBroadcasting();
    // Server2 gona join in.
    Zab zab2 = new Zab(st2, config2, server2, server1, state2, cb2, null);

    // While server2 is joining, we start sending 100 requests.
    for (int i = 0; i < 100; ++i) {
      zab1.send(ByteBuffer.wrap("txn".getBytes()), null);
    }
    st1.txnsCount.await();
    st2.txnsCount.await();
    // Consistency check.
    for (int i = 0; i < 100; ++i) {
      Assert.assertEquals(st1.deliveredTxns.get(i).getZxid(),
                          st2.deliveredTxns.get(i).getZxid());
    }
    zab1.shutdown();
    zab2.shutdown();
  }

  @Test(timeout=20000)
  public void testSendingWhileRemoving() throws Exception {
    // Tests keep sending request while someone is removed.
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st1 = new TestStateMachine(101);
    TestStateMachine st2 = new TestStateMachine(1);

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();

    PersistentState state1 = makeInitialState(server1, 0);
    PersistentState state2 = makeInitialState(server2, 0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st1, config1, server1, server1, state1, cb1, null);
    cb1.waitBroadcasting();

    // Send first request.
    zab1.send(ByteBuffer.wrap("txn".getBytes()), null);

    Zab zab2 = new Zab(st2, config2, server2, server1, state2, cb2, null);
    cb2.waitBroadcasting();
    // Server2 removes itself from cluster.
    zab2.remove(server2, null);

    for (int i = 0; i < 100; ++i) {
      zab1.send(ByteBuffer.wrap("txn".getBytes()), null);
    }
    // Waits server 1 delivers all the 101 transactions.
    st1.txnsCount.await();
    // Server 2 should exit eventually.
    cb2.waitExit();
    zab1.shutdown();
    zab2.shutdown();
  }

  @Test(timeout=20000)
  public void testLeaderExit() throws Exception {
    /*
     * Starts server1.
     * server2 joins server1.
     * server3 joins server1
     * server1 gets removed.
     *
     * Expecting server2 and server3 forms a new quorum and the configuration
     * should be only server2 and server3.
     */
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st1 = new TestStateMachine();
    TestStateMachine st2 = new TestStateMachine();
    TestStateMachine st3 = new TestStateMachine();

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();
    final String server3 = getUniqueHostPort();

    PersistentState state1 = makeInitialState(server1, 0);
    PersistentState state2 = makeInitialState(server2, 0);
    PersistentState state3 = makeInitialState(server3, 0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();
    ZabConfig config3 = new ZabConfig();

    Zab zab1 = new Zab(st1, config1, server1, server1, state1, cb1, null);
    cb1.waitBroadcasting();

    Zab zab2 = new Zab(st2, config2, server2, server1, state2, cb2, null);
    cb2.waitBroadcasting();
    st2.waitMembershipChanged();

    Zab zab3 = new Zab(st3, config3, server3, server1, state3, cb3, null);
    cb3.waitBroadcasting();
    st3.waitMembershipChanged();

    // Server1 exits.
    zab1.remove(server1, null);
    // Waits server1 exit.
    cb1.waitExit();

    // server2 and server3 should go back recovery and form a new quorum.
    cb3.waitBroadcasting();
    cb2.waitBroadcasting();
    st2.waitMembershipChanged();
    st3.waitMembershipChanged();

    // The cluster size should be 2.
    Assert.assertEquals(2, st2.clusters.size());
    Assert.assertEquals(2, st3.clusters.size());
    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }

  @Test(timeout=20000)
  public void testFlushFromLeader() throws Exception {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    TestStateMachine st1 = new TestStateMachine(2);
    final String server1 = getUniqueHostPort();

    PersistentState state1 = makeInitialState(server1, 0);
    ZabConfig config1 = new ZabConfig();

    Zab zab1 = new Zab(st1, config1, server1, server1, state1, cb1, null);
    cb1.waitBroadcasting();

    zab1.send(ByteBuffer.wrap("req1".getBytes()), null);
    zab1.flush(ByteBuffer.wrap("flush".getBytes()), null);

    st1.txnsCount.await();
    // Make sure first delivered txn is req1.
    Assert.assertEquals(ByteBuffer.wrap("req1".getBytes()),
                        st1.deliveredTxns.get(0).getBody());
    // Make sure last delivered txn is flush.
    Assert.assertEquals(ByteBuffer.wrap("flush".getBytes()),
                        st1.deliveredTxns.get(1).getBody());
    zab1.shutdown();
  }

  @Test(timeout=20000)
  public void testFlushFromFollower() throws Exception {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st1 = new TestStateMachine(3);
    TestStateMachine st2 = new TestStateMachine(5);

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();

    PersistentState state1 = makeInitialState(server1, 0);
    PersistentState state2 = makeInitialState(server2, 0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st1, config1, server1, server1, state1, cb1, null);
    Zab zab2 = new Zab(st2, config2, server2, server1, state2, cb2, null);
    cb2.waitBroadcasting();

    zab2.send(ByteBuffer.wrap("req1".getBytes()), null);
    zab2.flush(ByteBuffer.wrap("flush1".getBytes()), null);
    zab2.send(ByteBuffer.wrap("req2".getBytes()), null);
    zab2.flush(ByteBuffer.wrap("flush2".getBytes()), null);
    zab2.send(ByteBuffer.wrap("req3".getBytes()), null);

    st1.txnsCount.await();
    st2.txnsCount.await();
    // Leader should received 3 delivered txns.
    Assert.assertEquals(st1.deliveredTxns.size(), 3);
    // Follower should received 5 delivered txns(1 is FLUSH).
    Assert.assertEquals(st2.deliveredTxns.size(), 5);
    // Make sure the first delivered txn is req1.
    Assert.assertEquals(ByteBuffer.wrap("req1".getBytes()),
                        st2.deliveredTxns.get(0).getBody());
    // Make sure the second delivered txn is flush1.
    Assert.assertEquals(ByteBuffer.wrap("flush1".getBytes()),
                        st2.deliveredTxns.get(1).getBody());
    // Make sure the third delivered txn is req2.
    Assert.assertEquals(ByteBuffer.wrap("req2".getBytes()),
                        st2.deliveredTxns.get(2).getBody());
    // Make sure the fourth delivered txn is flush2.
    Assert.assertEquals(ByteBuffer.wrap("flush2".getBytes()),
                        st2.deliveredTxns.get(3).getBody());
    // Make sure the fifth delivered txn is req3.
    Assert.assertEquals(ByteBuffer.wrap("req3".getBytes()),
                        st2.deliveredTxns.get(4).getBody());
    zab1.shutdown();
    zab2.shutdown();
  }

  @Test(timeout=20000)
  public void testRetryJoin()
      throws IOException, InterruptedException {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st1 = new TestStateMachine(1);
    TestStateMachine st2 = new TestStateMachine(1);

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();

    PersistentState state1 = makeInitialState(server1, 0);
    PersistentState state2 = makeInitialState(server2, 0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab2 = new Zab(st2, config2, server1, server1, state2, cb2, null);

    // Waits for a while to make zab2 first joining fails.
    Thread.sleep(500);

    Zab zab1 = new Zab(st1, config1, server2, server1, state1, cb1, null);
    // Finally the joiner will join the cluster.
    cb2.waitBroadcasting();
    zab1.shutdown();
    zab2.shutdown();
  }

  @Test(timeout=20000)
  public void testRecoveryAfterJoin()
      throws IOException, InterruptedException {
    /*
     * Case 1 :
     *
     * 1. starts server1
     * 2. starts server2 join in server1
     * 3. starts server3 join in server1
     * 4. server3 crashes.
     *
     * 5 server3 recovers and finds out who is the leader.
     */
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st1 = new TestStateMachine();
    TestStateMachine st2 = new TestStateMachine();
    TestStateMachine st3 = new TestStateMachine();

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();
    final String server3 = getUniqueHostPort();

    PersistentState state1 = makeInitialState(server1, 0);
    PersistentState state2 = makeInitialState(server2, 0);
    PersistentState state3 = makeInitialState(server3, 0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();
    ZabConfig config3 = new ZabConfig();

    Zab zab1 = new Zab(st1, config1, server1, server1, state1, cb1, null);
    cb1.waitBroadcasting();

    Zab zab2 = new Zab(st2, config2, server2, server1, state2, cb2, null);
    cb2.waitBroadcasting();

    Zab zab3 = new Zab(st3, config3, server3, server1, state3, cb3, null);
    cb3.waitBroadcasting();

    // Simulates zab3 failures.
    zab3.shutdown();

    // Recovers from the log directory.
    cb3 = new QuorumTestCallback();
    zab3 = new Zab(st3, config3, state3, cb3, null);
    cb3.waitBroadcasting();

    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }

  @Test(expected=ZabException.InvalidPhase.class)
  public void testInvalidPhase() throws Exception {
    // Send request in non-broadcasting phase should raise exception.
    QuorumTestCallback cb = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);

    ZabConfig config1 = new ZabConfig();
    config1.setLogDir(new File(getDirectory(), server1).getPath());
    Zab zab1 = new Zab(st, config1, server1, peers);

    zab1.send(ByteBuffer.wrap("HelloWorld".getBytes()), null);
    zab1.shutdown();
  }

  @Test(timeout=20000)
  public void testContext() throws Exception {
    // State machine implementation.
    class CtxStateMachine  extends TestStateMachine {
      ArrayList<Object> sendCtxs = new ArrayList<Object>();
      ArrayList<Object> flushCtxs = new ArrayList<Object>();
      ArrayList<Object> removeCtxs = new ArrayList<Object>();
      ArrayList<Object> snapshotCtxs = new ArrayList<Object>();
      CountDownLatch condRemoved = new CountDownLatch(1);

      CtxStateMachine(int count) {
        super(count);
      }

      @Override
      public void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId,
                          Object ctx) {
        if (ctx != null) {
          sendCtxs.add(ctx);
        }
        super.deliver(zxid, stateUpdate, clientId, ctx);
      }

      @Override
      public void flushed(Zxid zxid, ByteBuffer flushReq, Object ctx) {
        flushCtxs.add(ctx);
        super.flushed(zxid, flushReq, ctx);
      }

      @Override
      public void removed(String serverId, Object ctx) {
        removeCtxs.add(ctx);
        condRemoved.countDown();
      }

      @Override
      public void snapshotDone(String filePath, Object ctx) {
        snapshotCtxs.add(ctx);
      }
    }
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();

    CtxStateMachine st1 = new CtxStateMachine(6);
    CtxStateMachine st2 = new CtxStateMachine(5);

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();
    PersistentState state1 = makeInitialState(server1, 0);
    state1.setProposedEpoch(1);
    state1.setAckEpoch(1);
    PersistentState state2 = makeInitialState(server2, 0);

    Zab zab1 = new Zab(st1, config1, server1, peers, state1, cb1, null);
    Zab zab2 = new Zab(st2, config2, server2, peers, state2, cb2, null);

    st1.waitMembershipChanged();
    cb1.waitBroadcasting();
    cb2.waitBroadcasting();

    zab1.send(ByteBuffer.wrap("HelloWorld1".getBytes()), "server1_1");
    zab1.send(ByteBuffer.wrap("HelloWorld2".getBytes()), "server1_2");
    zab1.send(ByteBuffer.wrap("HelloWorld3".getBytes()), "server1_3");
    zab1.flush(ByteBuffer.wrap("HelloWorld3".getBytes()), "server1_4");
    zab2.send(ByteBuffer.wrap("HelloWorld3".getBytes()), "server2_1");
    zab2.send(ByteBuffer.wrap("HelloWorld3".getBytes()), "server2_2");

    st1.txnsCount.await();
    st2.txnsCount.await();

    // There are 3 sent requests delivered with context on server1.
    Assert.assertEquals(3, st1.sendCtxs.size());
    // There are 1 flushed request delivered with context on server1.
    Assert.assertEquals(1, st1.flushCtxs.size());
    // There are 2 sent request delivered with context on server2.
    Assert.assertEquals(2, st2.sendCtxs.size());

    Assert.assertEquals((String)st1.sendCtxs.get(0), "server1_1");
    Assert.assertEquals((String)st1.sendCtxs.get(1), "server1_2");
    Assert.assertEquals((String)st1.sendCtxs.get(2), "server1_3");
    Assert.assertEquals((String)st2.sendCtxs.get(0), "server2_1");
    Assert.assertEquals((String)st2.sendCtxs.get(1), "server2_2");

    // Removes server2.
    zab1.remove(server2, "remove from server1");
    st1.condRemoved.await();

    // There are 1 remove request delivered with context on server1.
    Assert.assertEquals(1, st1.removeCtxs.size());
    zab1.shutdown();
    zab2.shutdown();
  }

  @Test(timeout=20000)
  public void testJoinWithoutSync() throws Exception {
    // Test joining without synchronization, starts 3 servers simultaneously
    // and verify in the end all of them get started.
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st1 = new TestStateMachine(1);
    TestStateMachine st2 = new TestStateMachine(1);
    TestStateMachine st3 = new TestStateMachine(1);

    final String server1 = getUniqueHostPort();
    final String server2 = getUniqueHostPort();
    final String server3 = getUniqueHostPort();

    PersistentState state1 = makeInitialState(server1, 0);
    PersistentState state2 = makeInitialState(server2, 0);
    PersistentState state3 = makeInitialState(server3, 0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();
    ZabConfig config3 = new ZabConfig();

    Zab zab1 = new Zab(st1, config1, server1, server1, state1, cb1, null);
    Thread.sleep(500);
    Zab zab2 = new Zab(st2, config2, server2, server1, state2, cb2, null);
    Zab zab3 = new Zab(st3, config3, server3, server1, state3, cb3, null);
    cb1.waitCopCommit();
    cb1.waitCopCommit();

    zab1.send(ByteBuffer.wrap("req1".getBytes()), null);
    // Waits for the transaction delivered.
    st1.txnsCount.await();
    st2.txnsCount.await();
    st3.txnsCount.await();

    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }
}
