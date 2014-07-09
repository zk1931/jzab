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

  // For debugging purpose.
  private final String serverId;

  public CommitProcessor(Log log,
                         StateMachine stateMachine,
                         String serverId) {
    this.log = log;
    this.stateMachine = stateMachine;
    this.serverId = serverId;
    // Starts running.
    ft = Executors.newSingleThreadExecutor().submit(this);
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
        LOG.debug("{} got commit request {}, last {}.",
                  this.serverId,
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
        this.lastDeliveredZxid = zxid;

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

                LOG.debug("{} delivers transaction : {}.",
                          this.serverId,
                          txn.getZxid());

                this.stateMachine.deliver(txn.getZxid(), txn.getBody());
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
}
