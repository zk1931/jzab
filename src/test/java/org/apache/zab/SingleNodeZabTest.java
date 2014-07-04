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

import java.util.concurrent.CountDownLatch;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dummy StateMachine implementation. Used for test only.
 */
class TestStateMachine implements StateMachine {
  ArrayList<Transaction> deliveredTxns = new ArrayList<Transaction>();

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
  public void deliver(Zxid zxid, ByteBuffer stateUpdate) {
    // Add the delivered message to list.
    LOG.debug("Delivers txn {}", zxid);
    this.deliveredTxns.add(new Transaction(zxid, stateUpdate));

    if (txnsCount != null) {
      txnsCount.countDown();
    }
  }

  @Override
  public void getState(OutputStream os) {
    throw new UnsupportedOperationException("This implementation"
        + "doesn't support getState operation");
  }

  @Override
  public void setState(InputStream is) {
    throw new UnsupportedOperationException("This implementation"
        + "doesn't support setState operation");
  }
}

/**
 * Tests on single node Zab implementation.
 */
public class SingleNodeZabTest extends TestBase {
  private SingleNodeZab zab;
  private TestStateMachine sm;
  private File logDir;

  /**
   * Tests setup.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    this.logDir = this.getDirectory();
    File logFile = new File(this.logDir, "transaction.log");
    this.sm = new TestStateMachine();
    Properties prop = new Properties();
    prop.setProperty("logdir", this.logDir.getCanonicalPath());
    this.zab = new SingleNodeZab(this.sm,
                                 prop);
  }

  /**
   * Tests if the messages are delivered.
   *
   * @throws IOException
   */
  @Test
  public void testDelivery() throws IOException {
    this.zab.send(ByteBuffer.wrap("message 0".getBytes()));
    this.zab.send(ByteBuffer.wrap("message 1".getBytes()));
    this.zab.send(ByteBuffer.wrap("message 2".getBytes()));
    this.zab.send(ByteBuffer.wrap("message 3".getBytes()));
    // Make sure there're 4 messages delivered.
    Assert.assertEquals(this.sm.deliveredTxns.size(), 4);
    // Assert first message is correct.
    Assert.assertTrue(this.sm.deliveredTxns.get(0).getBody()
                      .equals(ByteBuffer.wrap("message 0".getBytes())));
    // Assert second message is correct.
    Assert.assertTrue(this.sm.deliveredTxns.get(1).getBody()
                      .equals(ByteBuffer.wrap("message 1".getBytes())));
    // Assert third message is correct.
    Assert.assertTrue(this.sm.deliveredTxns.get(2).getBody()
                      .equals(ByteBuffer.wrap("message 2".getBytes())));
    // Assert fourth message is correct.
    Assert.assertTrue(this.sm.deliveredTxns.get(3).getBody()
                      .equals(ByteBuffer.wrap("message 3".getBytes())));
  }

  /**
   * Tests if the transactions are replayed.
   *
   * @throws IOException
   */
  @Test
  public void testReplayLog() throws IOException {
    this.zab.send(ByteBuffer.wrap("message 0".getBytes()));
    this.zab.send(ByteBuffer.wrap("message 1".getBytes()));
    // Replay from second transaction.
    this.zab.replayLogFrom(new Zxid(0, 1));
    // After replay, there should have 3 delivered messages in list.
    Assert.assertEquals(this.sm.deliveredTxns.size(), 3);
    // First message should be "message 0"
    Assert.assertTrue(this.sm.deliveredTxns.get(0).getBody()
                      .equals(ByteBuffer.wrap("message 0".getBytes())));
    // The last two messages should be identical.
    Assert.assertTrue(this.sm.deliveredTxns.get(1).getBody()
                      .equals(ByteBuffer.wrap("message 1".getBytes())));
    Assert.assertTrue(this.sm.deliveredTxns.get(2).getBody()
                      .equals(ByteBuffer.wrap("message 1".getBytes())));
  }

  /**
   * Tests if the transaction is replayed after recovery.
   *
   * @throws IOException
   */
  @Test
  public void testRecovery() throws IOException {
    this.zab.send(ByteBuffer.wrap("message 0".getBytes()));
    this.zab.send(ByteBuffer.wrap("message 1".getBytes()));

    // Simulate crash. Restart.
    this.sm = new TestStateMachine();
    Properties prop = new Properties();
    prop.setProperty("logdir", this.logDir.getCanonicalPath());
    // Recover from log file.
    this.zab = new SingleNodeZab(this.sm,
                                 prop);
    // After recovery, there should have 2 delivered messages in list.
    Assert.assertEquals(this.sm.deliveredTxns.size(), 2);
    // Assert that the redelivered messages are correct.
    Assert.assertTrue(this.sm.deliveredTxns.get(0).getBody()
                      .equals(ByteBuffer.wrap("message 0".getBytes())));
    Assert.assertTrue(this.sm.deliveredTxns.get(1).getBody()
                      .equals(ByteBuffer.wrap("message 1".getBytes())));
  }
}
