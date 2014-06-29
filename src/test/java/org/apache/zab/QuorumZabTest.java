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
import java.util.concurrent.CountDownLatch;
import org.junit.Assert;
import org.junit.Test;

/**
 * Used for tests.
 */
class QuorumTestCallback implements QuorumZab.StateChangeCallback {
  int establishedEpoch = -1;
  int ackknowledgedEpoch = -1;
  String electedLeader;
  String syncFollower;
  Zxid syncZxid;
  int syncAckEpoch;
  CountDownLatch count;


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
  public void leaderBroadcasting(int epoch) {
    this.ackknowledgedEpoch = epoch;
    this.count.countDown();
  }

  @Override
  public void followerBroadcasting(int epoch) {
    this.ackknowledgedEpoch = epoch;
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
                                     .setLog(new DummyLog(0));

    QuorumZab zab1 = new QuorumZab(null, cb, state1);

    QuorumZab.TestState state2 = new QuorumZab
                                     .TestState("server2",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(new DummyLog(5))
                                     .setProposedEpoch(2)
                                     .setAckEpoch(1);


    QuorumZab zab2 = new QuorumZab(null, null, state2);

    QuorumZab.TestState state3 = new QuorumZab
                                     .TestState("server3",
                                                "server1;server2;server3",
                                                getDirectory())
                                     .setLog(new DummyLog(5))
                                     .setProposedEpoch(2)
                                     .setAckEpoch(1);

    QuorumZab zab3 = new QuorumZab(null, null, state3);

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
}
