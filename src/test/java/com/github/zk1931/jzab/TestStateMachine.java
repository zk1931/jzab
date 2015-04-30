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

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dummy StateMachine implementation. Used for test only.
 */
class TestStateMachine implements StateMachine {
  ArrayList<Transaction> deliveredTxns = new ArrayList<Transaction>();
  Semaphore semMembership = new Semaphore(0);
  Set<String> clusters = null;
  Set<String> activeFollowers = null;
  String leader;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestStateMachine.class);

  /**
   * The expected delivered txns.
   */
  CountDownLatch txnsCount = null;

  public TestStateMachine() {
  }

  public TestStateMachine(int count) {
    txnsCount = new CountDownLatch(count);
  }

  @Override
  public ByteBuffer preprocess(Zxid zxid, ByteBuffer message) {
    // Just return the message without any processing.
    return message;
  }

  @Override
  public void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId,
                      Object ctx) {
    // Add the delivered message to list.
    LOG.debug("Delivers txn {}. Origin : {}", zxid, clientId);
    this.deliveredTxns.add(new Transaction(zxid, stateUpdate));
    if (txnsCount != null) {
      txnsCount.countDown();
    }
  }

  @Override
  public void flushed(Zxid zxid, ByteBuffer flushReq, Object ctx) {
    LOG.debug("Deliver syncReq {}.");
    this.deliveredTxns.add(new Transaction(zxid, flushReq));
    if (txnsCount != null) {
      txnsCount.countDown();
    }
  }

  @Override
  public void removed(String serverId, Object ctx) {
  }

  @Override
  public void save(FileOutputStream fos) {
    throw new UnsupportedOperationException("This implementation"
        + "doesn't support getState operation");
  }

  @Override
  public void snapshotDone(String filePath, Object ctx) {
  }

  @Override
  public void restore(FileInputStream fis) {
    throw new UnsupportedOperationException("This implementation"
        + "doesn't support setState operation");
  }

  @Override
  public void recovering(PendingRequests pendings) {}

  @Override
  public void leading(Set<String> followers, Set<String> clusterMembers) {
    this.activeFollowers = followers;
    this.clusters = clusterMembers;
    LOG.debug("Leader got membership callback");
    semMembership.release();
  }

  @Override
  public void following(String ld, Set<String> clusterMembers) {
    this.leader = ld;
    this.clusters = clusterMembers;
    LOG.debug("Follower got membership callback");
    semMembership.release();
  }

  void waitMembershipChanged() throws InterruptedException {
    semMembership.acquire();
  }
}
