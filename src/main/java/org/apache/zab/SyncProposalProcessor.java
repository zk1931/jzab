/**
 * Licensed to the Apache Software Foundatlion (ASF) under one
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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the logic synchronizing the transactions to disk. It can
 * batch several transactions and sync to disk once. The next request processor
 * should be AckProcessor to acknowledge the synchronized transactions.
 */
public class SyncProposalProcessor implements RequestProcessor,
                                              Callable<Integer> {

  private final Log log;

  private final RequestProcessor nextProcessor;

  private final BlockingQueue<Request> proposalQueue =
      new LinkedBlockingQueue<Request>();

  Future<Integer> ft;

  private static final Logger LOG =
      LoggerFactory.getLogger(SyncProposalProcessor.class);

  /**
   * Constructs a SyncProposalProcessor object.
   *
   * @param log the log which the transaction will be synchronized to.
   * @param nextProcessor next request processor. (it should be AckProcessor)
   */
  public SyncProposalProcessor(Log log, RequestProcessor nextProcessor) {
    this.log = log;
    this.nextProcessor = nextProcessor;
    // Starts running.
    ft = Executors.newSingleThreadExecutor().submit(this);
  }

  @Override
  public void processRequest(Request request) {
    proposalQueue.add(request);
  }

  @Override
  public Integer call() throws Exception {

    LOG.debug("SyncRequestProcessor gets started.");

    while (true) {

      Request request = this.proposalQueue.take();

      Transaction txn = MessageBuilder
                        .fromProposal(request.getMessage().getProposal());

      LOG.debug("Syncing transaction {} to disk.", txn);
      this.log.append(txn);
      this.log.sync();

      // Passe to AckProcessor.
      this.nextProcessor.processRequest(request);
    }
  }

}
