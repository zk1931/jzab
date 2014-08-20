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

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.Properties;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

class SnapshotStateMachine implements StateMachine {
  private static final Logger LOG =
      LoggerFactory.getLogger(SnapshotStateMachine.class);

  ConcurrentHashMap<String, String> state =
    new ConcurrentHashMap<String, String>();

  CountDownLatch txnsCount;

  Semaphore semMembership = new Semaphore(0);

  SnapshotStateMachine(int nTxns) {
    txnsCount = new CountDownLatch(nTxns);
  }

  @Override
  public ByteBuffer preprocess(Zxid zxid, ByteBuffer message) {
    return message;
  }

  @Override
  public void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId) {
    byte[] buffer = new byte[stateUpdate.remaining()];
    stateUpdate.get(buffer);
    String key = new String(buffer);
    LOG.debug("Delivering {}", key);
    this.state.put(key, "value");
    txnsCount.countDown();
  }

  @Override
  public void save(OutputStream os) {
    LOG.debug("SAVE is called.");
    try {
      ObjectOutputStream out = new ObjectOutputStream(os);
      out.writeObject(state);
    } catch (IOException e) {
      LOG.error("Caught exception", e);
    }
  }

  @Override
  public void restore(InputStream is) {
    LOG.debug("RESTORE is called.");
    try {
      ObjectInputStream oin = new ObjectInputStream(is);
      state = (ConcurrentHashMap<String, String>)oin.readObject();
      LOG.debug("The size of map after recovery from snapshot file is {}",
                state.size());
    } catch (Exception e) {
      LOG.error("Caught exception", e);
    }
  }

  @Override
  public void recovering() {}

  @Override
  public void leading(Set<String> followers, Set<String> members) {
    semMembership.release();
  }

  @Override
  public void following(String ld, Set<String> members) {
    semMembership.release();
  }

  void waitMemberChanged() throws InterruptedException {
    semMembership.acquire();
  }
}


/**
 * Tests for Snapshot.
 */
public class SnapshotTest extends TestBase {
  private static final Logger LOG =
    LoggerFactory.getLogger(SnapshotTest.class);

  @Test
  public void testSnapshotSingleServer() throws Exception {
    final int nTxns = 20;
    QuorumTestCallback cb1 = new QuorumTestCallback();
    SnapshotStateMachine st1 = new SnapshotStateMachine(nTxns);
    String server = getUniqueHostPort();
    Properties prop = new Properties();
    // For testing purpose, set the threshold to 32 bytes..
    prop.setProperty("snapshot_threshold", "32");
    prop.setProperty("serverId", server);
    prop.setProperty("logdir", getDirectory().getPath());
    QuorumZab zab = new QuorumZab(st1, prop, server);
    for (int i = 0; i < nTxns; ++i) {
      zab.send(ByteBuffer.wrap(("txns" + i).getBytes()));
      // Sleep a while to avoid all the transactions batch together.
      Thread.sleep(50);
    }
    st1.txnsCount.await();
    zab.shutdown();
    SnapshotStateMachine stNew = new SnapshotStateMachine(nTxns);
    prop = new Properties();
    prop.setProperty("logdir", getDirectory().getPath());
    zab = new QuorumZab(stNew, prop);
    stNew.waitMemberChanged();
    Assert.assertEquals(st1.state, stNew.state);
    zab.shutdown();
  }

  @Test
  public void testSnapshotCluster() throws Exception {
    final int nTxns = 20;
    QuorumTestCallback cb1 = new QuorumTestCallback();
    SnapshotStateMachine st1 = new SnapshotStateMachine(nTxns);
    QuorumTestCallback cb2 = new QuorumTestCallback();
    SnapshotStateMachine st2 = new SnapshotStateMachine(nTxns);
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    Properties prop1 = new Properties();
    // For testing purpose, set the threshold to 32 bytes..
    prop1.setProperty("snapshot_threshold_bytes", "32");
    prop1.setProperty("serverId", server1);
    prop1.setProperty("logdir",
                      getDirectory().getPath() + File.separator + server1);
    Properties prop2 = new Properties();
    // For testing purpose, set the threshold to 32 bytes..
    prop2.setProperty("snapshot_threshold_bytes", "32");
    prop2.setProperty("serverId", server2);
    prop2.setProperty("logdir",
                      getDirectory().getPath() + File.separator + server2);
    QuorumZab zab1 = new QuorumZab(st1, prop1, server1);
    st1.waitMemberChanged();
    // Server2 joins in.
    QuorumZab zab2 = new QuorumZab(st2, prop2, server1);
    st2.waitMemberChanged();
    st1.waitMemberChanged();
    for (int i = 0; i < nTxns; ++i) {
      zab1.send(ByteBuffer.wrap(("txns" + i).getBytes()));
      // Sleep a while to avoid all the transactions batch together.
      Thread.sleep(50);
    }
    st1.txnsCount.await();
    st2.txnsCount.await();
    // Shutdown both servers.
    zab1.shutdown();
    zab2.shutdown();

    // Restarts them.
    SnapshotStateMachine stNew1 = new SnapshotStateMachine(nTxns);
    SnapshotStateMachine stNew2 = new SnapshotStateMachine(nTxns);
    prop1 = new Properties();
    prop1.setProperty("logdir",
                      getDirectory().getPath() + File.separator + server1);
    prop2 = new Properties();
    prop2.setProperty("logdir",
                      getDirectory().getPath() + File.separator + server2);
    zab1 = new QuorumZab(stNew1, prop1);
    zab2 = new QuorumZab(stNew2, prop2);
    stNew1.waitMemberChanged();
    stNew2.waitMemberChanged();
    // Make sure the states are consistent.
    Assert.assertEquals(st1.state, stNew1.state);
    Assert.assertEquals(st2.state, stNew2.state);
    Assert.assertEquals(stNew1.state, stNew2.state);
    zab1.shutdown();
    zab2.shutdown();
  }

  @Test
  public void testSnapshotSynchronization() throws Exception {
    // Starts server1, sends transactions txn1,txn2 ... txnn.
    // Starts server2 joins server1, the snapshot will be used to synchronize
    // server2. In the end, we verify the two state machines have the same state
    final int nTxns = 50;
    QuorumTestCallback cb1 = new QuorumTestCallback();
    SnapshotStateMachine st1 = new SnapshotStateMachine(nTxns);
    QuorumTestCallback cb2 = new QuorumTestCallback();
    SnapshotStateMachine st2 = new SnapshotStateMachine(nTxns);
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    Properties prop1 = new Properties();
    // For testing purpose, set the threshold to 64 bytes..
    prop1.setProperty("snapshot_threshold_bytes", "64");
    prop1.setProperty("serverId", server1);
    prop1.setProperty("logdir",
                      getDirectory().getPath() + File.separator + server1);
    Properties prop2 = new Properties();
    // For testing purpose, set the threshold to 32 bytes..
    prop2.setProperty("serverId", server2);
    prop2.setProperty("logdir",
                      getDirectory().getPath() + File.separator + server2);
    QuorumZab zab1 = new QuorumZab(st1, prop1, server1);
    st1.waitMemberChanged();
    for (int i = 0; i < nTxns; ++i) {
      zab1.send(ByteBuffer.wrap(("txns" + i).getBytes()));
      // Sleep a while to avoid all the transactions batch together.
      Thread.sleep(20);
    }
    st1.txnsCount.await();
    // Server2 joins in.
    QuorumZab zab2 = new QuorumZab(st2, prop2, server1);
    st2.waitMemberChanged();
    Assert.assertEquals(st1.state, st2.state);
    zab1.shutdown();
    zab2.shutdown();
  }
}
