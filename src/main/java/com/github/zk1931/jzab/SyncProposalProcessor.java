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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import com.github.zk1931.jzab.proto.ZabMessage.Message;
import com.github.zk1931.jzab.proto.ZabMessage.Message.MessageType;
import com.github.zk1931.jzab.proto.ZabMessage.Proposal.ProposalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the logic synchronizing the transactions to disk. It can
 * batch several transactions and sync to disk once.
 */
class SyncProposalProcessor implements RequestProcessor, Callable<Void> {

  private final Log log;

  private final PersistentState persistence;

  private final BlockingQueue<MessageTuple> proposalQueue =
      new LinkedBlockingQueue<MessageTuple>();

  Future<Void> ft;

  private final Transport transport;

  private static final Logger LOG =
      LoggerFactory.getLogger(SyncProposalProcessor.class);

  // The maximum count of batched transactions. Once batched transactions are
  // beyond this size, we force synchronizing them to disk and acknowledging
  // the leader.
  private final int maxBatchSize;

  private final String serverId;

  /**
   * Constructs a SyncProposalProcessor object.
   *
   * @param persistence the persistent variables.
   * @param transport used to send acknowledgment.
   * @param maxBatchSize the maximum batch size.
   * @throws IOException in case of IO failure.
   */
  public SyncProposalProcessor(PersistentState persistence, Transport transport,
                               int maxBatchSize) throws IOException {
    this.persistence = persistence;
    this.log = persistence.getLog();
    this.transport = transport;
    this.maxBatchSize = maxBatchSize;
    this.serverId = persistence.getLastSeenConfig().getServerId();
    ExecutorService es =
        Executors.newSingleThreadExecutor(DaemonThreadFactory.FACTORY);
    ft = es.submit(this);
    es.shutdown();
  }

  @Override
  public void processRequest(MessageTuple request) {
    proposalQueue.add(request);
  }

  void sendAck(String source, Zxid ackZxid) {
    Message ack = MessageBuilder.buildAck(ackZxid);
    this.transport.send(source, ack);
  }

  @Override
  public Void call() throws Exception {
    try {
      LOG.debug("Batched SyncRequestProcessor gets started.");
      MessageTuple lastReq = null;
      // Number of transactions batched so far.
      int batchCount = 0;

      while (true) {
        MessageTuple req;
        if (lastReq == null) {
          req = this.proposalQueue.take();
        } else {
          req = this.proposalQueue.poll();
          if (req == null ||
              batchCount == maxBatchSize ||
              req == MessageTuple.REQUEST_OF_DEATH) {
            // Sync to disk and send ACK to leader.
            this.log.sync();
            Zxid zxid = MessageBuilder
                        .fromProtoZxid(lastReq.getMessage()
                                              .getProposal()
                                              .getZxid());
            sendAck(lastReq.getServerId(), zxid);
            batchCount = 0;
          }
        }
        if (req == MessageTuple.REQUEST_OF_DEATH) {
          break;
        }
        if (req == null) {
          lastReq = null;
          continue;
        }
        if (req.getMessage().getType() == MessageType.PROPOSAL) {
          // It's PROPOSAL, sync to disk.
          Message msg = req.getMessage();
          Transaction txn = MessageBuilder
                            .fromProposal(msg.getProposal());
          // TODO : avoid this?
          ByteBuffer body = txn.getBody().asReadOnlyBuffer();
          LOG.debug("Syncing transaction {} to disk.", txn.getZxid());
          batchCount++;
          lastReq = req;
          if (txn.getType() == ProposalType.COP_VALUE) {
            // If it's COP, we should also update cluster_config file.
            ClusterConfiguration cnf =
              ClusterConfiguration.fromByteBuffer(body, this.serverId);
            persistence.setLastSeenConfig(cnf);
            // If it's COP, we shouldn't batch it, send ACK immediatly.
            sendAck(req.getServerId(), txn.getZxid());
            batchCount = 0;
            lastReq = null;
          }
          // If it's COP we need to write to cluster_config file first then
          // append to log in case of failures between writing to cluster_config
          // and appending to log. In this case cluster_config is one more txn
          // ahead the log file and we'll just delete it when we restart Zab.
          this.log.append(txn);
        }
      }
    } catch (Exception e) {
      LOG.error("Caught an exception in SyncProposalProcessor", e);
      throw e;
    }
    LOG.debug("SyncProposalProcessor has been shut down.");
    return null;
  }

  @Override
  public void shutdown() throws InterruptedException, ExecutionException {
    this.proposalQueue.add(MessageTuple.REQUEST_OF_DEATH);
    this.ft.get();
  }
}
