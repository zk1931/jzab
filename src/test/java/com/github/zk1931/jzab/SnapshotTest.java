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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
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
  Semaphore semSnapshot = new Semaphore(0);

  SnapshotStateMachine(int nTxns) {
    txnsCount = new CountDownLatch(nTxns);
  }

  @Override
  public ByteBuffer preprocess(Zxid zxid, ByteBuffer message) {
    return message;
  }

  @Override
  public void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId,
                      Object ctx) {
    byte[] buffer = new byte[stateUpdate.remaining()];
    stateUpdate.get(buffer);
    String key = new String(buffer);
    LOG.debug("Delivering {}", key);
    this.state.put(key, "value");
    txnsCount.countDown();
  }

  @Override
  public void flushed(Zxid zxid, ByteBuffer flushReq, Object ctx) {}

  @Override
  public void save(FileOutputStream fos) {
    LOG.debug("SAVE is called.");
    try {
      ObjectOutputStream out = new ObjectOutputStream(fos);
      out.writeObject(state);
    } catch (IOException e) {
      LOG.error("Caught exception", e);
    }
  }

  @Override
  public void snapshotDone(String filePath, Object ctx) {
    LOG.debug("Snapshot is stored at {}", filePath);
    semSnapshot.release();
  }

  @Override
  public void restore(FileInputStream fis) {
    LOG.debug("RESTORE is called.");
    try {
      ObjectInputStream oin = new ObjectInputStream(fis);
      ConcurrentHashMap<?, ?> map = (ConcurrentHashMap<?, ?>)oin.readObject();
      state = new ConcurrentHashMap<String, String>();
      Iterator it = map.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry pairs = (Map.Entry)it.next();
        state.put((String)pairs.getKey(), (String)pairs.getValue());
      }
      LOG.debug("The size of map after recovery from snapshot file is {}",
                state.size());
    } catch (Exception e) {
      LOG.error("Caught exception", e);
    }
  }

  @Override
  public void removed(String serverId, Object ctx) {
  }

  @Override
  public void recovering(PendingRequests pendings) {}

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

  void waitSnapshot() throws InterruptedException {
    semSnapshot.acquire();
  }
}

/**
 * Tests for Snapshot.
 */
public class SnapshotTest extends TestBase {
  private static final Logger LOG =
    LoggerFactory.getLogger(SnapshotTest.class);

  @Test(timeout=10000)
  public void testSnapshotSingleServer() throws Exception {
    final int nTxns = 20;
    QuorumTestCallback cb1 = new QuorumTestCallback();
    SnapshotStateMachine st1 = new SnapshotStateMachine(nTxns);
    String server = getUniqueHostPort();

    ZabConfig config = new ZabConfig();
    // For testing purpose, set the threshold to 32 bytes..
    config.setLogDir(getDirectory().getPath());

    Zab zab = new Zab(st1, config, server, server);
    st1.waitMemberChanged();

    for (int i = 0; i < nTxns; ++i) {
      zab.send(ByteBuffer.wrap(("txns" + i).getBytes()), null);
    }
    st1.txnsCount.await();
    // Take the snapshot after all transaction gets delivered.
    zab.takeSnapshot(null);
    st1.waitSnapshot();

    Thread.sleep(1000);

    zab.shutdown();
    SnapshotStateMachine stNew = new SnapshotStateMachine(nTxns);
    zab = new Zab(stNew, config);
    stNew.waitMemberChanged();
    // Make sure restored state is consistent.
    Assert.assertEquals(st1.state, stNew.state);
    zab.shutdown();
  }

  @Test(timeout=20000)
  public void testSnapshotCluster() throws Exception {
    final int nTxns = 20;
    QuorumTestCallback cb1 = new QuorumTestCallback();
    SnapshotStateMachine st1 = new SnapshotStateMachine(nTxns);
    QuorumTestCallback cb2 = new QuorumTestCallback();
    SnapshotStateMachine st2 = new SnapshotStateMachine(nTxns);
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();
    config1.setLogDir(getDirectory().getPath() + File.separator + server1);
    config2.setLogDir(getDirectory().getPath() + File.separator + server2);

    Zab zab1 = new Zab(st1, config1, server1, server1);
    st1.waitMemberChanged();

    Zab zab2 = new Zab(st2, config2, server2, server1);
    st2.waitMemberChanged();
    int snapIdx = new Random().nextInt(nTxns);
    for (int i = 0; i < nTxns; ++i) {
      zab1.send(ByteBuffer.wrap(("txns" + i).getBytes()), null);
      Thread.sleep(5);
      if (i == snapIdx) {
        zab1.takeSnapshot(null);
        zab2.takeSnapshot(null);
        st1.waitSnapshot();
        st2.waitSnapshot();
      }
    }
    st1.txnsCount.await();
    st2.txnsCount.await();
    // Shutdown both servers.
    zab1.shutdown();
    zab2.shutdown();

    // Restarts them.
    SnapshotStateMachine stNew1 = new SnapshotStateMachine(nTxns);
    SnapshotStateMachine stNew2 = new SnapshotStateMachine(nTxns);
    zab1 = new Zab(stNew1, config1);
    zab2 = new Zab(stNew2, config2);
    stNew1.waitMemberChanged();
    stNew2.waitMemberChanged();
    // Make sure the states are consistent.
    Assert.assertEquals(st1.state, stNew1.state);
    Assert.assertEquals(st2.state, stNew2.state);
    Assert.assertEquals(stNew1.state, stNew2.state);
    zab1.shutdown();
    zab2.shutdown();
  }

  @Test(timeout=20000)
  public void testSnapshotSynchronizationCase1() throws Exception {
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

    ZabConfig config1 = new ZabConfig();
    config1.setLogDir(getDirectory().getPath() + File.separator + server1);
    ZabConfig config2 = new ZabConfig();
    config2.setLogDir(getDirectory().getPath() + File.separator + server2);

    Zab zab1 = new Zab(st1, config1, server1, server1);
    st1.waitMemberChanged();

    int snapIdx = new Random().nextInt(nTxns);
    for (int i = 0; i < nTxns; ++i) {
      zab1.send(ByteBuffer.wrap(("txns" + i).getBytes()), null);
      // Sleep a while to avoid all the transactions batch together.
      Thread.sleep(5);
      if (i == snapIdx) {
        zab1.takeSnapshot(null);
        st1.waitSnapshot();
      }
    }
    st1.txnsCount.await();
    // Server2 joins in.
    Zab zab2 = new Zab(st2, config2, server2, server1);
    st2.waitMemberChanged();
    Assert.assertEquals(st1.state, st2.state);
    zab2.shutdown();

    st2 = new SnapshotStateMachine(nTxns);
    // zab2 recovers.
    zab2 = new Zab(st2, config2);
    st2.waitMemberChanged();
    // After recovery, we verify they still have same states.
    Assert.assertEquals(st1.state, st2.state);
    zab2.shutdown();
    zab1.shutdown();
  }

  @Test(timeout=20000)
  public void testSnapshotSynchronizationCase2() throws Exception {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    SnapshotStateMachine st1 = new SnapshotStateMachine(0);
    SnapshotStateMachine st2 = new SnapshotStateMachine(0);

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);
    PersistentState state1 = makeInitialState(server1, 5);
    state1.setAckEpoch(0);

    PersistentState state2 = makeInitialState(server2, 1);
    state2.setAckEpoch(0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st1, config1, server1, peers, state1, cb1, null);
    Zab zab2 = new Zab(st2, config2, server2, peers, state2, cb2, null);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();
    Assert.assertEquals(cb1.initialHistory.size(), 5);
    Assert.assertEquals(cb1.initialHistory.get(0).getZxid(), new Zxid(0, 0));
    Assert.assertTrue(cb2.initialHistory.size() == 5);
    Assert.assertEquals(cb2.initialHistory.get(0).getZxid(), new Zxid(0, 0));

    // server1 gonna take snapshot.
    zab1.takeSnapshot(null);
    st1.waitSnapshot();
    // Make sure server1 does take snapshot.
    Assert.assertEquals(new Zxid(0, 4), state1.getSnapshotZxid());

    // Shutdown zab2.
    zab2.shutdown();
    // Mannuly truncate all the logs of server2.
    state2.getLog().truncate(Zxid.ZXID_NOT_EXIST);

    st2 = new SnapshotStateMachine(0);
    cb2 = new QuorumTestCallback();
    // Restarts server2.
    zab2 = new Zab(st2, config2, state2, cb2, null);
    cb2.waitBroadcasting();
    // server2 should get snapshot file synchronized from server1.
    Assert.assertEquals(new Zxid(0, 4), state2.getSnapshotZxid());
    // Eventually they will have same state.
    Assert.assertEquals(st2.state, st1.state);

    zab1.shutdown();
    zab2.shutdown();
  }

  @Test(timeout=20000)
  public void testSnapshotSynchronizationCase3() throws Exception {
    QuorumTestCallback cb1 = new QuorumTestCallback();
    QuorumTestCallback cb2 = new QuorumTestCallback();
    SnapshotStateMachine st1 = new SnapshotStateMachine(0);
    SnapshotStateMachine st2 = new SnapshotStateMachine(0);

    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();
    Set<String> peers = new HashSet<>();
    peers.add(server1);
    peers.add(server2);
    peers.add(server3);
    PersistentState state1 = makeInitialState(server1, 5);
    state1.setProposedEpoch(2);
    state1.setAckEpoch(2);

    PersistentState state2 = makeInitialState(server2, 1);
    state2.setAckEpoch(0);

    ZabConfig config1 = new ZabConfig();
    ZabConfig config2 = new ZabConfig();

    Zab zab1 = new Zab(st1, config1, server1, peers, state1, cb1, null);
    Zab zab2 = new Zab(st2, config2, server2, peers, state2, cb2, null);

    cb1.waitBroadcasting();
    cb2.waitBroadcasting();
    Assert.assertEquals(cb1.initialHistory.size(), 5);
    Assert.assertEquals(cb1.initialHistory.get(0).getZxid(), new Zxid(0, 0));
    Assert.assertTrue(cb2.initialHistory.size() == 5);
    Assert.assertEquals(cb2.initialHistory.get(0).getZxid(), new Zxid(0, 0));

    // server1 takes snapshot.
    zab1.takeSnapshot(null);
    st1.waitSnapshot();
    // Make sure server1 did take snapshot.
    Assert.assertEquals(new Zxid(0, 4), state1.getSnapshotZxid());

    // Shutdown zab2.
    zab2.shutdown();
    // Add one more transaction to make server1 and server2 have different
    // epochs.
    appendTxns(state2.getLog(), new Zxid(1, 0), 1);
    // Reset epoch number to make sure server1 becomes leader.
    state2.setProposedEpoch(0);
    state2.setAckEpoch(0);

    st2 = new SnapshotStateMachine(0);
    cb2 = new QuorumTestCallback();
    // Restarts server2.
    zab2 = new Zab(st2, config2, state2, cb2, null);
    cb2.waitBroadcasting();
    // server2 should get snapshot file synchronized from server1.
    Assert.assertEquals(new Zxid(0, 4), state2.getSnapshotZxid());
    // Eventuall they will have same state.
    Assert.assertEquals(st2.state, st1.state);
    zab1.shutdown();
    zab2.shutdown();
  }
}
