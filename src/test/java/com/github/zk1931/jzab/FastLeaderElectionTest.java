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
import java.util.HashSet;
import java.util.Set;

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
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);

    PersistentState state1 = makeInitialState(server1, 1);
    state1.setAckEpoch(0);

    PersistentState state2 = makeInitialState(server2, 1);
    state2.setAckEpoch(0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st, config1, server1, peers, state1, cb1, null);
    Zab zab2 = new Zab(st, config2, server2, peers, state2, cb2, null);

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
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);

    PersistentState state1 = makeInitialState(server1, 1);
    state1.setProposedEpoch(1);
    state1.setAckEpoch(1);

    PersistentState state2 = makeInitialState(server2, 1);
    state2.setAckEpoch(0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st, config1, server1, peers, state1, cb1, null);
    Zab zab2 = new Zab(st, config2, server2, peers, state2, cb2, null);

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
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);

    PersistentState state1 = makeInitialState(server1, 1);
    PersistentState state2 = makeInitialState(server2, 0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st, config1, server1, peers, state1, cb1, null);
    Zab zab2 = new Zab(st, config2, server2, peers, state2, cb2, null);

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
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);

    PersistentState state1 = makeInitialState(server1, 0);
    state1.setAckEpoch(2);
    state1.setProposedEpoch(2);

    PersistentState state2 = makeInitialState(server2, 0);
    state2.setAckEpoch(1);
    state2.setProposedEpoch(1);

    PersistentState state3 = makeInitialState(server3, 0);
    state2.setAckEpoch(0);
    state2.setProposedEpoch(0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();
    ZabConfig config3 = new ZabConfig();

    Zab zab1 = new Zab(st, config1, server1, peers, state1, cb1, null);
    Zab zab2 = new Zab(st, config2, server2, peers, state2, cb2, null);
    Zab zab3 = new Zab(st, config3, server3, peers, state3, cb3, null);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();
    cb3.waitBroadcasting();
    // Verify they have selected the same leader.
    Assert.assertEquals(cb2.electedLeader , cb1.electedLeader);
    Assert.assertEquals(cb2.electedLeader , cb3.electedLeader);
    // The leader should be either server1 or server2.
    Assert.assertTrue(server1.equals(cb1.electedLeader) ||
                      server3.equals(cb1.electedLeader));

    zab1.shutdown();
    zab2.shutdown();
    zab3.shutdown();
  }
}
