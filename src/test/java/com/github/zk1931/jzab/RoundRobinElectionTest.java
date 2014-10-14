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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 * Utility class used to make PersistentState object for testing.
 */
final class DummyPersistentState {

  private DummyPersistentState() {}

  static PersistentState make(File directory, String peers, long epoch)
      throws IOException {
    PersistentState persistence = new PersistentState(directory);
    List<String> peerList = Arrays.asList(peers.split(";"));
    ClusterConfiguration cnf =
      new ClusterConfiguration(new Zxid(0, 0), peerList, "host");
    persistence.setLastSeenConfig(cnf);
    persistence.setProposedEpoch(epoch);
    return persistence;
  }

  static PersistentState make(File directory, String peers)
      throws IOException {
    return make(directory, peers, -1);
  }
}

/**
 * Tests round robin election algorithm.
 */
public class RoundRobinElectionTest extends TestBase {
  /**
   * Tests the dummy server state.
   */
  @Test
  public void testDummyPersistentState() throws Exception {
    PersistentState ps
      = DummyPersistentState.make(getDirectory(), "server1;server2", 1);
    List<String> peers = ps.getLastSeenConfig().getPeers();
    Assert.assertEquals("server1", peers.get(0));
    Assert.assertEquals("server2", peers.get(1));
    Assert.assertEquals(2, peers.size());
    Assert.assertEquals(ps.getProposedEpoch(), 1);
  }

  /**
   * Tests single round round robin election.
   */
  @Test
  public void testSingleRoundElection() throws Exception {
    PersistentState ps
      = DummyPersistentState.make(getDirectory(), "server1;server2");
    Election election = new RoundRobinElection(ps);
    String electedLeader = election.electLeader();
    Assert.assertEquals(electedLeader, "server1");
  }

  /**
   * Tests multiple rounds round robin election.
   */
  @Test
  public void testMultiRoundsElection() throws Exception {
    PersistentState ps
      = DummyPersistentState.make(getDirectory(), "server1;server2;server3");
    Election election = new RoundRobinElection(ps);
    String electedLeader = election.electLeader();
    // First round should select server 1.
    Assert.assertEquals(electedLeader, "server1");
    electedLeader = election.electLeader();
    // Second round should select server 2.
    Assert.assertEquals(electedLeader, "server2");
    electedLeader = election.electLeader();
    // Third round should select server 3.
    Assert.assertEquals(electedLeader, "server3");
  }

  /**
   * Tests if the round number is reset to zero after epoch changes.
   */
  @Test
  public void testResetRound() throws Exception {
    PersistentState ps
      = DummyPersistentState.make(getDirectory(), "server1;server2");
    Election election = new RoundRobinElection(ps);
    // First round election, the round number will be set to 1.
    String electedLeader = election.electLeader();
    Assert.assertEquals(electedLeader, "server1");
    // Increase epoch number by 1.
    DummyPersistentState.make(getDirectory(), "server1;server2", 0);
    electedLeader = election.electLeader();
    // Now since the round number is reset to 0, it should select server1.
    Assert.assertEquals(electedLeader, "server1");
    // Third election.
    electedLeader = election.electLeader();
    // Since the epoch remains same, it should select server2.
    Assert.assertEquals(electedLeader, "server2");
  }
}
