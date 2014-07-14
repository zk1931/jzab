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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.zab.proto.ZabMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to deliver committed transaction.
 */
public class CommitProcessor implements RequestProcessor,
                                        Callable<Void> {

  private final BlockingQueue<Request> commitQueue =
      new LinkedBlockingQueue<Request>();

  private static final Logger LOG =
      LoggerFactory.getLogger(CommitProcessor.class);

  private final Log log;

  private final StateMachine stateMachine;

  private Zxid lastDeliveredZxid = Zxid.ZXID_NOT_EXIST;

  Future<Void> ft;

  /**
   * Constructs a CommitProcessor.
   *
   * @param log the log.
   * @param stateMachine the state machine of application.
   * @param lastDeliveredZxid the last delivered zxid, CommitProcessor won't
   * deliver any transactions which are smaller or equal than this zxid.
   */
  public CommitProcessor(Log log,
                         StateMachine stateMachine,
                         Zxid lastDeliveredZxid) {
    this.log = log;
    this.stateMachine = stateMachine;
    this.lastDeliveredZxid = lastDeliveredZxid;
    ExecutorService es =
        Executors.newSingleThreadExecutor(DaemonThreadFactory.FACTORY);
    ft = es.submit(this);
    es.shutdown();
  }

  @Override
  public void processRequest(Request request) {
    this.commitQueue.add(request);
  }

  @Override
  public Void call() throws Exception {
    LOG.debug("CommitProcessor gets started.");
    try {
      while (true) {
        Request request = this.commitQueue.take();
        if (request == Request.REQUEST_OF_DEATH) {
          break;
        }
        ZabMessage.Commit commit = request.getMessage().getCommit();
        Zxid zxid = MessageBuilder.fromProtoZxid(commit.getZxid());
        LOG.debug("Received a commit request {}, last {}.",
                  zxid,
                  this.lastDeliveredZxid);
        if (zxid.compareTo(this.lastDeliveredZxid) == 0) {
          // Since leader may send duplicated committed zxid, we probably want
          // to avoid deliver duplicated transactions even the transactions are
          // idempotent.
          LOG.debug("{} is duplicated COMMIT message with last {}",
                    zxid,
                    this.lastDeliveredZxid);
          continue;
        }

        Zxid tZxid = this.lastDeliveredZxid;

        synchronized(this.log) {
          // It will deliver all the transactions from last committed point.
          try (Log.LogIterator iter = this.log.getIterator(tZxid)) {
            if (iter.hasNext()) {
              if (tZxid != Zxid.ZXID_NOT_EXIST) {
                // Skip last one.
                iter.next();
              }
              while (iter.hasNext()) {
                Transaction txn = iter.next();
                if (txn.getZxid().compareTo(zxid) > 0) {
                  break;
                }
                LOG.debug("Delivering transaction {}.", txn.getZxid());
                this.stateMachine.deliver(txn.getZxid(), txn.getBody());
                this.lastDeliveredZxid = txn.getZxid();
              }
            }
          }
        }
      }
    } catch (RuntimeException e) {
      LOG.error("Caught exception in CommitProcessor!", e);
      throw e;
    }
    LOG.debug("CommitProcessor has been shut down.");
    return null;
  }

  @Override
  public void shutdown() throws InterruptedException, ExecutionException {
    this.commitQueue.add(Request.REQUEST_OF_DEATH);
    this.ft.get();
  }

  public Zxid getLastDeliveredZxid() {
    return this.lastDeliveredZxid;
  }
}
