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
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

class SnapshotProcessorStateMachine implements StateMachine {
  private static final Logger LOG =
      LoggerFactory.getLogger(SnapshotProcessorStateMachine.class);

  Serializable mockState = null;
  CountDownLatch countSave = new CountDownLatch(1);

  public SnapshotProcessorStateMachine(Serializable mockState) {
    this.mockState = mockState;
  }

  @Override
  public ByteBuffer preprocess(Zxid zxid, ByteBuffer message) {
    return message;
  }

  @Override
  public void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId) {}

  @Override
  public void save(OutputStream os) {
    try {
      ObjectOutputStream out = new ObjectOutputStream(os);
      out.writeObject(mockState);
    } catch (IOException e) {
      LOG.error("Caught exception", e);
    }
    countSave.countDown();
  }

  @Override
  public void restore(InputStream is) {}

  @Override
  public void recovering() {}

  @Override
  public void leading(Set<String> followers, Set<String> members) {}

  @Override
  public void following(String ld, Set<String> members) {}
}

/**
 * Tests for SnapshotProcessor.
 */
public class SnapshotProcessorTest extends TestBase {
  private static final Logger LOG =
    LoggerFactory.getLogger(SnapshotProcessorTest.class);

  @Test
  public void testSnapshot() throws Exception {
    // Tests taking a snapshot of HashSet and restore it from snapshot file.
    HashSet<String> state = new HashSet<String>();
    state.add("test1");
    state.add("test2");
    state.add("test3");
    PersistentState persistence = new PersistentState(getDirectory());
    SnapshotProcessorStateMachine st =
      new SnapshotProcessorStateMachine(state);
    SnapshotProcessor sp = new SnapshotProcessor(st, persistence);
    MessageTuple tuple =
      new MessageTuple("test", MessageBuilder.buildSnapshot(new Zxid(0, 10)));
    sp.processRequest(tuple);
    st.countSave.await();
    sp.shutdown();
    File snap = persistence.getSnapshotFile();
    ObjectInputStream oi = new ObjectInputStream(new FileInputStream(snap));
    HashSet<String> stateRestored = (HashSet<String>)oi.readObject();
    // Make sure the snapshot is correct.
    Assert.assertEquals(state, stateRestored);
  }
}
