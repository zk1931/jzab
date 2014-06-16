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
import java.io.File;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation of single node Zab. There's
 * no leader election and network message for this
 * implementation. It's used for implementing some
 * referencing applications without waiting for the
 * fully functional Zab.
 */
public class SingleNodeZab extends Zab {
  private final Log txnLog;
  // Last proposed transaction
  private Zxid lastProposedZxid;

  static final Logger LOG = LoggerFactory.getLogger(SingleNodeZab.class);

  /**
   * Construct the single node zab.
   *
   * @param st the implementation of StateMachine interface.
   * @param dir the directory of logs
   * @throws IOException in case of IO failure
   */
  public SingleNodeZab(StateMachine st, String dir) throws IOException {
    super(st);
    this.txnLog = new SimpleLog(new File(dir, "transaction.log"));
    try {
      this.lastProposedZxid = txnLog.getLatestZxid();
      // If the lastProposedZxid is not Zxid.ZXID_NOT_EXIST, it means the
      // log file is not empty. Replay all the transactions in the log.
      if (this.lastProposedZxid.compareTo(Zxid.ZXID_NOT_EXIST) != 0) {
        this.replayLogFrom(new Zxid(0, 0));
      }
    } catch(IOException e) {
      LOG.error("Failed to replay transaction.log from Zxid 0:0", e);
      txnLog.close();
      throw e;
    }
  }

  /**
   * Sends a message to Zab. The single node Zab simply call
   * preprocess to get state update then call deliver.
   *
   * @param message message to send through Zab
   * @throws IOException in case of IO failures
   */
  @Override
  public void send(ByteBuffer message) throws IOException {
    Zxid zxid = new Zxid(this.lastProposedZxid.getEpoch(),
                         this.lastProposedZxid.getXid() + 1);
    // Updates last proposed transaction id.
    this.lastProposedZxid = zxid;
    // Converts the message to state update.
    ByteBuffer stateUpdate = this.stateMachine.preprocess(zxid, message);
    // Appends it to transaction log.
    this.txnLog.append(new Transaction(zxid, stateUpdate.asReadOnlyBuffer()));
    // Sync log to physical media.
    this.txnLog.sync();
    // Delivers the state update.
    this.stateMachine.deliver(zxid, stateUpdate);
  }

  /**
   * Trims a prefix of the log. Used to reduce the size
   * of log after snapshot. It's not implemented in this version.
   *
   * @param zxid trim the log to zxid(including zxid)
   */
  @Override
  public void trimLogTo(Zxid zxid) {
    throw new UnsupportedOperationException("Now the log doesn't support "
        + "trim operation");
  }

  /**
   * Redelivers all txns starting from zxid.
   *
   * @param zxid the first transaction id to replay from.
   * @throws IOException in case of IO failures
   */
  @Override
  public void replayLogFrom(Zxid zxid) throws IOException {
    try (Log.LogIterator iter = this.txnLog.getIterator(zxid)) {
      while (iter.hasNext()) {
        Transaction txn = iter.next();
        this.stateMachine.deliver(txn.getZxid(), txn.getBody());
      }
    }
  }
}
