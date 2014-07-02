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
import java.util.concurrent.CountDownLatch;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

/**
 * Used for tests.
 */
class QuorumTestCallback implements QuorumZab.StateChangeCallback {
  int establishedEpoch = -1;
  int acknowledgedEpoch = -1;
  String electedLeader;
  String syncFollower;
  Zxid syncZxid;
  int syncAckEpoch;
  CountDownLatch count;
  List<Transaction> initialHistory = new ArrayList<Transaction>();


  QuorumTestCallback(int phaseCount) {
    this.count = new CountDownLatch(phaseCount);
  }

  @Override
  public void electing() {
    this.count.countDown();
  }

  @Override
  public void leaderDiscovering(String leader) {
    this.electedLeader = leader;
    this.count.countDown();
  }

  @Override
  public void followerDiscovering(String leader) {
    this.electedLeader = leader;
    this.count.countDown();
  }

  @Override
  public void initialHistoryOwner(String server, int aEpoch, Zxid zxid) {
    this.syncFollower = server;
    this.syncZxid =zxid;
    this.syncAckEpoch =aEpoch;
  }

  @Override
  public void leaderSynchronizating(int epoch) {
    this.establishedEpoch = epoch;
    this.count.countDown();
  }

  @Override
  public void followerSynchronizating(int epoch) {
    this.establishedEpoch = epoch;
    this.count.countDown();
  }

  @Override
  public void leaderBroadcasting(int epoch, List<Transaction> history) {
    this.acknowledgedEpoch = epoch;
    this.initialHistory = history;
    this.count.countDown();
  }

  @Override
  public void followerBroadcasting(int epoch, List<Transaction> history) {
    this.acknowledgedEpoch = epoch;
    this.initialHistory = history;
    this.count.countDown();
  }
}

/**
 * QuorumZab Test.
 */
public class QuorumZabTest extends TestBase  {

  /**
   * Test if the new epoch is established.
   *
   * @throws InterruptedException
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=1000)
  public void testEstablishNewEpoch() throws InterruptedException, IOException {
    // 4 phase changes : electing -> discovering -> sync -> broadcasting
    QuorumTestCallback cb = new QuorumTestCallback(4);

    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState("server1",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(new DummyLog(10))
                                     .setAckEpoch(0);

    QuorumZab zab1 = new QuorumZab(null, cb, state1, null);

    DummyLog log = new DummyLog(5);

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState("server2",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(log)
                                     .setProposedEpoch(2)
                                     .setAckEpoch(1);


    QuorumZab zab2 = new QuorumZab(null, null, state2, null);

    QuorumZab.TestState state3 = new QuorumZab
                                     .TestState("server3",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(log)
                                     .setProposedEpoch(2)
                                     .setAckEpoch(1);

    QuorumZab zab3 = new QuorumZab(null, null, state3, null);

    cb.count.await();
    // The established epoch should be 3.
    Assert.assertEquals(cb.establishedEpoch, 3);

    // The elected leader should be server1.
    Assert.assertEquals(cb.electedLeader, "server1");

    // server 2 and server 3 should have the "best" history.
    Assert.assertTrue(cb.syncFollower.equals("server2") ||
                      cb.syncFollower.equals("server3"));

    // The last zxid of the owner of initial history should be (0, 4)
    Assert.assertEquals(cb.syncZxid.compareTo(new Zxid(0, 4)), 0);

    // The last ack epoch of the owner of initial history should be 1.
    Assert.assertEquals(cb.syncAckEpoch, 1);
  }

  /**
   * Make sure the leader can start up by itself.
   */
  @Test(timeout=1000)
  public void testSingleServer() throws InterruptedException, IOException {
    // 4 phase changes : electing -> discovering -> sync -> broadcasting
    QuorumTestCallback cb = new QuorumTestCallback(4);

    QuorumZab.TestState state = new QuorumZab
                                    .TestState("server1",
                                               "server1",
                                               getDirectory())
                                    .setLog(new DummyLog(0));
    QuorumZab zab1 = new QuorumZab(null, cb, state, null);

    cb.count.await();
    Assert.assertEquals(-1, cb.acknowledgedEpoch);
    Assert.assertEquals(0, cb.establishedEpoch);
    Assert.assertEquals("server1", cb.electedLeader);
    Assert.assertEquals("server1", cb.syncFollower);
    Assert.assertTrue(cb.initialHistory.isEmpty());
    Assert.assertEquals(0, cb.syncZxid.compareTo(Zxid.ZXID_NOT_EXIST));
  }

  /**
   * Test synchronization case 1.
   *
   * @throws InterruptedException
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=1000)
  public void testSynchronizationCase1()
      throws InterruptedException, IOException {
    // 4 phase changes : electing -> discovering -> sync -> broadcasting
    QuorumTestCallback cb1 = new QuorumTestCallback(4);
    QuorumTestCallback cb2 = new QuorumTestCallback(4);

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
                                     .TestState("server1",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(new DummyLog(1))
                                     .setAckEpoch(0);

    QuorumZab zab1 = new QuorumZab(null, cb1, state1, null);

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState("server2",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(new DummyLog(1))
                                     .setAckEpoch(0);

    QuorumZab zab2 = new QuorumZab(null, cb2, state2, null);

    cb1.count.await();
    cb2.count.await();

    Assert.assertEquals(cb1.initialHistory.size(), 1);
    Assert.assertEquals(cb1.initialHistory.get(0).getZxid(), new Zxid(0, 0));

    Assert.assertTrue(cb2.initialHistory.size() == 1);
    Assert.assertEquals(cb2.initialHistory.get(0).getZxid(), new Zxid(0, 0));

  }

  /**
   * Test synchronization case 2.
   *
   * @throws InterruptedException
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=1000)
  public void testSynchronizationCase2()
      throws InterruptedException, IOException {
    // 4 phase changes : electing -> discovering -> sync -> broadcasting
    QuorumTestCallback cb1 = new QuorumTestCallback(4);
    QuorumTestCallback cb2 = new QuorumTestCallback(4);

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
                                     .TestState("server1",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(0);

    QuorumZab zab1 = new QuorumZab(null, cb1, state1, null);

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState("server2",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(new DummyLog(0))
                                     .setAckEpoch(0);

    QuorumZab zab2 = new QuorumZab(null, cb2, state2, null);

    cb1.count.await();
    cb2.count.await();

    Assert.assertEquals(cb1.initialHistory.size(), 2);
    Assert.assertEquals(cb1.initialHistory.get(0).getZxid(), new Zxid(0, 0));
    Assert.assertEquals(cb1.initialHistory.get(1).getZxid(), new Zxid(0, 1));

    Assert.assertTrue(cb2.initialHistory.size() == 2);
    Assert.assertEquals(cb2.initialHistory.get(0).getZxid(), new Zxid(0, 0));
    Assert.assertEquals(cb2.initialHistory.get(1).getZxid(), new Zxid(0, 1));
  }

  /**
   * Test synchronization case 3.
   *
   * @throws InterruptedException
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=1000)
  public void testSynchronizationCase3()
      throws InterruptedException, IOException {
    // 4 phase changes : electing -> discovering -> sync -> broadcasting
    QuorumTestCallback cb1 = new QuorumTestCallback(4);
    QuorumTestCallback cb2 = new QuorumTestCallback(4);

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
                                     .TestState("server1",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(new DummyLog(0))
                                     .setAckEpoch(0);

    QuorumZab zab1 = new QuorumZab(null, cb1, state1, null);


    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState("server2",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(0);

    QuorumZab zab2 = new QuorumZab(null, cb2, state2, null);

    cb1.count.await();
    cb2.count.await();

    Assert.assertEquals(cb1.initialHistory.size(), 2);
    Assert.assertEquals(cb1.initialHistory.get(0).getZxid(), new Zxid(0, 0));
    Assert.assertEquals(cb1.initialHistory.get(1).getZxid(), new Zxid(0, 1));

    Assert.assertTrue(cb2.initialHistory.size() == 2);
    Assert.assertEquals(cb2.initialHistory.get(0).getZxid(), new Zxid(0, 0));
    Assert.assertEquals(cb2.initialHistory.get(1).getZxid(), new Zxid(0, 1));
  }

  /**
   * Test synchronization case 4.
   *
   * @throws InterruptedException
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=1000)
  public void testSynchronizationCase4()
      throws InterruptedException, IOException {
    // 4 phase changes : electing -> discovering -> sync -> broadcasting
    QuorumTestCallback cb1 = new QuorumTestCallback(4);
    QuorumTestCallback cb2 = new QuorumTestCallback(4);

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
                                     .TestState("server1",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(log)
                                     .setAckEpoch(1);

    QuorumZab zab1 = new QuorumZab(null, cb1, state1, null);

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState("server2",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(2);

    QuorumZab zab2 = new QuorumZab(null, cb2, state2, null);

    cb1.count.await();
    cb2.count.await();

    Assert.assertEquals(cb1.initialHistory.size(), 2);
    Assert.assertEquals(cb1.initialHistory.get(0).getZxid(), new Zxid(0, 0));
    Assert.assertEquals(cb1.initialHistory.get(1).getZxid(), new Zxid(0, 1));

    Assert.assertTrue(cb2.initialHistory.size() == 2);
    Assert.assertEquals(cb2.initialHistory.get(0).getZxid(), new Zxid(0, 0));
    Assert.assertEquals(cb2.initialHistory.get(1).getZxid(), new Zxid(0, 1));
  }

  /**
   * Test synchronization case 5.
   *
   * @throws InterruptedException
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=1000)
  public void testSynchronizationCase5()
      throws InterruptedException, IOException {
    // 4 phase changes : electing -> discovering -> sync -> broadcasting
    QuorumTestCallback cb1 = new QuorumTestCallback(4);
    QuorumTestCallback cb2 = new QuorumTestCallback(4);

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
                                     .TestState("server1",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(0);

    QuorumZab zab1 = new QuorumZab(null, cb1, state1, null);

    DummyLog log = new DummyLog(1);
    log.append(new Transaction(new Zxid(1, 0),
                               ByteBuffer.wrap("1,0".getBytes())));

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState("server2",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(log)
                                     .setAckEpoch(1);

    QuorumZab zab2 = new QuorumZab(null, cb2, state2, null);

    cb1.count.await();
    cb2.count.await();

    Assert.assertEquals(cb1.initialHistory.size(), 2);
    Assert.assertEquals(cb1.initialHistory.get(0).getZxid(), new Zxid(0, 0));
    Assert.assertEquals(cb1.initialHistory.get(1).getZxid(), new Zxid(1, 0));

    Assert.assertTrue(cb2.initialHistory.size() == 2);
    Assert.assertEquals(cb2.initialHistory.get(0).getZxid(), new Zxid(0, 0));
    Assert.assertEquals(cb2.initialHistory.get(1).getZxid(), new Zxid(1, 0));
  }

  /**
   * Test synchronization case 6.
   *
   * @throws InterruptedException
   * @throws IOException in case of IO failure.
   */
  @Test(timeout=1000)
  public void testSynchronizationCase6()
      throws InterruptedException, IOException {
    // 4 phase changes : electing -> discovering -> sync -> broadcasting
    QuorumTestCallback cb1 = new QuorumTestCallback(4);
    QuorumTestCallback cb2 = new QuorumTestCallback(4);

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
                                     .TestState("server1",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(new DummyLog(0))
                                     .setAckEpoch(1);

    QuorumZab zab1 = new QuorumZab(null, cb1, state1, null);

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState("server2",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(0);

    QuorumZab zab2 = new QuorumZab(null, cb2, state2, null);

    cb1.count.await();
    cb2.count.await();

    Assert.assertEquals(cb1.initialHistory.size(), 0);
    Assert.assertEquals(cb2.initialHistory.size(), 0);
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
  @Test(timeout=1000)
  public void testSynchronizationCase7()
      throws InterruptedException, IOException {
    // 4 phase changes : electing -> discovering -> sync -> broadcasting
    QuorumTestCallback cb1 = new QuorumTestCallback(4);
    QuorumTestCallback cb2 = new QuorumTestCallback(4);
    QuorumZab.TestState state1 = new QuorumZab
                                     .TestState("server1",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(new DummyLog(3))
                                     .setAckEpoch(0);
    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState("server2",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(new DummyLog(2))
                                     .setAckEpoch(0);
    QuorumZab zab1 = new QuorumZab(null, cb1, state1, null);
    QuorumZab zab2 = new QuorumZab(null, cb2, state2, null);
    cb1.count.await();
    cb2.count.await();
    Assert.assertEquals(3, cb1.initialHistory.size());
    Assert.assertEquals(3, cb2.initialHistory.size());
  }
}
