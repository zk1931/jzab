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

import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 * Fakes the server state for test purpose only.
 */
class DummyServerState implements ServerState {
  List<String> serverList;
  int lastProposedEpoch = -1;

  public DummyServerState(String servers) {
    this.serverList = Arrays.asList(servers.split(";"));
  }

  @Override
  public int getEnsembleSize() {
    return this.serverList.size();
  }

  @Override
  public List<String> getServerList() {
    return this.serverList;
  }

  @Override
  public int getQuorumSize() {
    return getEnsembleSize() / 2 + 1;
  }

  @Override
  public int getProposedEpoch() {
    return this.lastProposedEpoch;
  }
}

/**
 * Dummy election callback, used for test purpose only.
 */
class DummyElectionCallback implements Election.ElectionCallback {
  String electedLeader = null;

  @Override
  public void leaderElected(String serverId) {
    this.electedLeader = serverId;
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
  public void testServerState() {
    DummyServerState st = new DummyServerState("server1;server2");
    Assert.assertEquals(st.serverList.get(0), "server1");
    Assert.assertEquals(st.serverList.get(1), "server2");
    Assert.assertEquals(st.getEnsembleSize(), 2);
  }

  /**
   * Tests single round round robin election.
   */
  @Test
  public void testSingleRoundElection() {
    DummyServerState st = new DummyServerState("server1;server2");
    DummyElectionCallback cb = new DummyElectionCallback();
    Election election = new RoundRobinElection();
    election.initialize(st, cb);
    Assert.assertEquals(cb.electedLeader, "server1");
  }

  /**
   * Tests multiple rounds round robin election.
   */
  @Test
  public void testMultiRoundsElection() {
    DummyServerState st = new DummyServerState("server1;server2;server3");
    DummyElectionCallback cb = new DummyElectionCallback();
    Election election = new RoundRobinElection();
    election.initialize(st, cb);
    // First round should select server 1.
    Assert.assertEquals(cb.electedLeader, "server1");
    election.initialize(st, cb);
    // Second round should select server 2.
    Assert.assertEquals(cb.electedLeader, "server2");
    election.initialize(st, cb);
    // Third round should select server 3.
    Assert.assertEquals(cb.electedLeader, "server3");
  }

  /**
   * Tests if the round number is reset to zero after epoch changes.
   */
  @Test
  public void testResetRound() {
    DummyServerState st = new DummyServerState("server1;server2;server3");
    DummyElectionCallback cb = new DummyElectionCallback();
    Election election = new RoundRobinElection();
    // First round election, the round number will be set to 1.
    election.initialize(st, cb);
    Assert.assertEquals(cb.electedLeader, "server1");
    // Increase epoch number by 1.
    st.lastProposedEpoch = st.lastProposedEpoch + 1;
    election.initialize(st, cb);
    // Now since the round number is reset to 0, it should select server1.
    Assert.assertEquals(cb.electedLeader, "server1");
    // Third election.
    election.initialize(st, cb);
    // Since the epoch remains same, it should select server2.
    Assert.assertEquals(cb.electedLeader, "server2");
  }
}
