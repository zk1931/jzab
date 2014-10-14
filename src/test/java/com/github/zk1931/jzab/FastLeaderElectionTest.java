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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for FastLeaderElection.
 */
public class FastLeaderElectionTest extends TestBase {

  private static final Logger LOG =
    LoggerFactory.getLogger(FastLeaderElectionTest.class);

  @Test(timeout=5000)
  public void testSelectLeaderWithHigherId() throws Exception {
    // Starts 2 servers from a 3-server cluster, they have the same
    // ackEpoch and lastZxid, we will pick the one with larger server id,
    // which is server2.
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    String servers = server1 + ";" + server2 + ";" + server3;

    Zab.TestState state1 = new Zab.TestState(server1,
                                             servers,
                                             getDirectory())
                                  .setLog(new DummyLog(1))
                                  .setAckEpoch(0);
    Zab zab1 = new Zab(st, cb1, null, state1);

    Zab.TestState state2 = new Zab.TestState(server2,
                                             servers,
                                             getDirectory())
                                  .setLog(new DummyLog(1))
                                  .setAckEpoch(0);
    Zab zab2 = new Zab(st, cb2, null, state2);

    cb1.waitDiscovering();
    cb2.waitDiscovering();
    // Verify we have selected the server with higher server id.
    Assert.assertEquals(server2, cb1.electedLeader);
    Assert.assertEquals(server2, cb2.electedLeader);
    zab1.shutdown();
    zab2.shutdown();
  }

  @Test(timeout=5000)
  public void testSelectLeaderWithHigherAckEpoch() throws Exception {
    // Starts 2 servers from a 3-server cluster, server1 has the same zxid
    // as server2 but with a higher ackEpoch, we will pick the server1 as the
    // leader.
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    String servers = server1 + ";" + server2 + ";" + server3;

    Zab.TestState state1 = new Zab.TestState(server1,
                                             servers,
                                             getDirectory())
                                  .setLog(new DummyLog(1))
                                  .setAckEpoch(1);
    Zab zab1 = new Zab(st, cb1, null, state1);

    Zab.TestState state2 = new Zab.TestState(server2,
                                             servers,
                                             getDirectory())
                                  .setLog(new DummyLog(1))
                                  .setAckEpoch(0);
    Zab zab2 = new Zab(st, cb2, null, state2);

    cb1.waitDiscovering();
    cb2.waitDiscovering();
    // Verify we have selected the server with higher server id.
    Assert.assertEquals(server1, cb1.electedLeader);
    Assert.assertEquals(server1, cb2.electedLeader);
    zab1.shutdown();
    zab2.shutdown();
  }

  @Test(timeout=5000)
  public void testSelectLeaderWithHigherZxid() throws Exception {
    // Starts 2 servers from a 3-server cluster, server1 has the same ackEpoch
    // as server2 but with a higher lastZxid, we will pick the server1 as the
    // leader.
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    String servers = server1 + ";" + server2 + ";" + server3;

    Zab.TestState state1 = new Zab.TestState(server1,
                                             servers,
                                             getDirectory())
                                  .setLog(new DummyLog(2))
                                  .setAckEpoch(0);
    Zab zab1 = new Zab(st, cb1, null, state1);

    Zab.TestState state2 = new Zab.TestState(server2,
                                             servers,
                                             getDirectory())
                                  .setLog(new DummyLog(1))
                                  .setAckEpoch(0);
    Zab zab2 = new Zab(st, cb2, null, state2);

    cb1.waitDiscovering();
    cb2.waitDiscovering();
    // Verify we have selected the server with higher server id.
    Assert.assertEquals(server1, cb1.electedLeader);
    Assert.assertEquals(server1, cb2.electedLeader);
    zab1.shutdown();
    zab2.shutdown();
  }

  @Test(timeout=5000)
  public void testSelectLeaderFromThreeServers() throws Exception {
    // Starts 3 servers from a 3-server cluster:
    //    server1 :  ackEpoch : 2, lastZxid (0, 0)
    //    server2 :  ackEpoch : 1, lastZxid (0, 0)
    //    server3 :  ackEpoch : 0, lastZxid (0, 0)
    // The best case we'll select server1 as the leader, the worst case
    // (If server2 and server3 form a quorum first) we'll select server2 as
    // the leader.
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    QuorumTestCallback cb3 = new QuorumTestCallback();
    TestStateMachine st = new TestStateMachine();

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    String servers = server1 + ";" + server2 + ";" + server3;

    Zab.TestState state1 = new Zab.TestState(server1,
                                             servers,
                                             getDirectory())
                                  .setLog(new DummyLog(2))
                                  .setAckEpoch(0);
    Zab zab1 = new Zab(st, cb1, null, state1);

    Zab.TestState state2 = new Zab.TestState(server2,
                                             servers,
                                             getDirectory())
                                  .setLog(new DummyLog(1))
                                  .setAckEpoch(0);
    Zab zab2 = new Zab(st, cb2, null, state2);

    Zab.TestState state3 = new Zab.TestState(server3,
                                             servers,
                                             getDirectory())
                                  .setLog(new DummyLog(1))
                                  .setAckEpoch(0);
    Zab zab3 = new Zab(st, cb3, null, state3);

    cb1.waitDiscovering();
    cb2.waitDiscovering();
    cb3.waitDiscovering();
    // Verify they have selected the same leader.
    Assert.assertEquals(cb2.electedLeader , cb1.electedLeader);
    Assert.assertEquals(cb2.electedLeader , cb3.electedLeader);
    // The leader should be either server1 or server2.
    Assert.assertTrue(server1.equals(cb1.electedLeader) ||
                      server2.equals(cb1.electedLeader));

    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }
}
