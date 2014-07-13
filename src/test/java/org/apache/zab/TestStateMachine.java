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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
