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
import java.io.InputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.Iterator;
import java.util.Map;
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
  public void flushed(ByteBuffer flushReq) {}

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

  @Test(timeout=10000)
  public void testSnapshotSingleServer() throws Exception {
    final int nTxns = 20;
    QuorumTestCallback cb1 = new QuorumTestCallback();
    SnapshotStateMachine st1 = new SnapshotStateMachine(nTxns);
    String server = getUniqueHostPort();

    ZabConfig config = new ZabConfig();
    // For testing purpose, set the threshold to 32 bytes..
    config.setSnapshotThreshold(32);
    config.setServerId(server);
    config.setLogDir(getDirectory().getPath());

    Zab zab = new Zab(st1, config, server);
    st1.waitMemberChanged();

    for (int i = 0; i < nTxns; ++i) {
      zab.send(ByteBuffer.wrap(("txns" + i).getBytes()));
      // Sleep a while to avoid all the transactions batch together.
      Thread.sleep(50);
    }
    st1.txnsCount.await();
    zab.shutdown();

    SnapshotStateMachine stNew = new SnapshotStateMachine(nTxns);
    zab = new Zab(stNew, config);
    stNew.waitMemberChanged();
    Assert.assertEquals(st1.state, stNew.state);
    zab.shutdown();
  }

  @Test(timeout=30000)
  public void testSnapshotCluster() throws Exception {
    final int nTxns = 20;
    QuorumTestCallback cb1 = new QuorumTestCallback();
    SnapshotStateMachine st1 = new SnapshotStateMachine(nTxns);
    QuorumTestCallback cb2 = new QuorumTestCallback();
    SnapshotStateMachine st2 = new SnapshotStateMachine(nTxns);
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();

    ZabConfig config1 = new ZabConfig();
    // For testing purpose, set the threshold to 32 bytes..
    config1.setSnapshotThreshold(32);
    config1.setServerId(server1);
    config1.setLogDir(getDirectory().getPath() + File.separator + server1);

    ZabConfig config2 = new ZabConfig();
    // For testing purpose, set the threshold to 32 bytes..
    config2.setSnapshotThreshold(32);
    config2.setServerId(server2);
    config2.setLogDir(getDirectory().getPath() + File.separator + server2);

    Zab zab1 = new Zab(st1, config1, server1);
    st1.waitMemberChanged();

    Zab zab2 = new Zab(st2, config2, server1);
    st2.waitMemberChanged();

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

  @Test(timeout=30000)
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

    ZabConfig config1 = new ZabConfig();
    // In order to reproduce the bug we found in snapshot transferring, we set
    // the threshold to 290 bytes. In this case, both the last zxid and snap
    // zxid will be (0, 50), which triggered the bug in old code.
    config1.setSnapshotThreshold(290);
    config1.setServerId(server1);
    config1.setLogDir(getDirectory().getPath() + File.separator + server1);

    ZabConfig config2 = new ZabConfig();
    config2.setServerId(server2);
    config2.setLogDir(getDirectory().getPath() + File.separator + server2);

    Zab zab1 = new Zab(st1, config1, server1);
    st1.waitMemberChanged();

    for (int i = 0; i < nTxns; ++i) {
      zab1.send(ByteBuffer.wrap(("txns" + i).getBytes()));
      // Sleep a while to avoid all the transactions batch together.
      Thread.sleep(5);
    }
    st1.txnsCount.await();
    // Server2 joins in.
    Zab zab2 = new Zab(st2, config2, server1);
    st2.waitMemberChanged();
    Assert.assertEquals(st1.state, st2.state);
    zab1.shutdown();
    zab2.shutdown();
  }
}
