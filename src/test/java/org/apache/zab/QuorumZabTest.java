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

import java.util.Properties;
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
  public void leaderSynchronizating(int epoch, String follower) {
    this.establishedEpoch = epoch;
    this.syncFollower = follower;
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
   */
  @Test(timeout=1000)
  public void testEstablishNewEpoch() throws InterruptedException {
    // 4 phase changes : electing -> discovering -> sync -> broadcasting
    QuorumTestCallback cb = new QuorumTestCallback(4);

    Properties p1 = new Properties();
    p1.setProperty("serverId", "server1");
    p1.setProperty("servers", "server1;server2;server3");
    QuorumZab zab1 = new QuorumZab(null, p1, cb, new QuorumZab.TestState());

    Properties p2 = new Properties();
    p2.setProperty("serverId", "server2");
    p2.setProperty("servers", "server1;server2;server3");
    QuorumZab zab2 = new QuorumZab(null,
                                   p2,
                                   null,
                                   new QuorumZab.TestState()
                                                .setProposedEpoch(2)
                                                .setAckEpoch(1));

    Properties p3 = new Properties();
    p3.setProperty("serverId", "server3");
    p3.setProperty("servers", "server1;server2;server3");
    QuorumZab zab3 = new QuorumZab(null,
                                   p3,
                                   null,
                                   new QuorumZab.TestState()
                                   .setProposedEpoch(2)
                                   .setAckEpoch(1));

    cb.count.await();
    // The established epoch should be 3.
    Assert.assertEquals(cb.establishedEpoch, 3);

    // The elected leader should be server1.
    Assert.assertEquals(cb.electedLeader, "server1");

    // server 2 and server 3 should have the "best" history.
    Assert.assertTrue(cb.syncFollower.equals("server2") ||
                      cb.syncFollower.equals("server3"));
  }
}
