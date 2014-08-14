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

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.zab.proto.ZabMessage;
import org.apache.zab.proto.ZabMessage.Message;
import org.apache.zab.proto.ZabMessage.Message.MessageType;
import org.apache.zab.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.zab.proto.ZabMessage.Proposal.ProposalType;

/**
 * This class is used to deliver committed transaction.
 */
public class CommitProcessor implements RequestProcessor,
                                        Callable<Void> {

  private final BlockingQueue<MessageTuple> commitQueue =
      new LinkedBlockingQueue<MessageTuple>();

  private static final Logger LOG =
      LoggerFactory.getLogger(CommitProcessor.class);

  private final StateMachine stateMachine;

  private Zxid lastDeliveredZxid = Zxid.ZXID_NOT_EXIST;

  private final List<ZabMessage.Proposal> pendingTxns =
      new LinkedList<ZabMessage.Proposal>();

  private final String serverId;

  private final Transport transport;

  Future<Void> ft;

  /**
   * Constructs a CommitProcessor. The CommitProcesor accepts both COMMIT and
   * PROPOSAL message. It puts the PROPOSAL into pendingTxn list and delivers
   * transactions from this list.
   *
   * @param stateMachine the state machine of application.
   * @param lastDeliveredZxid the last delivered zxid, CommitProcessor won't
   * deliver any transactions which are smaller or equal than this zxid.
   * @param serverId the id of the Participant.
   * @param transport the Transport object.
   */
  public CommitProcessor(StateMachine stateMachine,
                         Zxid lastDeliveredZxid,
                         String serverId,
                         Transport transport) {
    this.stateMachine = stateMachine;
    this.lastDeliveredZxid = lastDeliveredZxid;
    this.serverId = serverId;
    this.transport = transport;
    ExecutorService es =
        Executors.newSingleThreadExecutor(DaemonThreadFactory.FACTORY);
    ft = es.submit(this);
    es.shutdown();
  }

  @Override
  public void processRequest(MessageTuple request) {
    this.commitQueue.add(request);
  }

  @Override
  public Void call() throws Exception {
    LOG.debug("CommitProcessor gets started.");
    try {
      while (true) {
        MessageTuple request = this.commitQueue.take();
        if (request == MessageTuple.REQUEST_OF_DEATH) {
          break;
        }
        Message msg = request.getMessage();
        if (msg.getType() == MessageType.PROPOSAL) {
          // Puts the proposal in queue.
          this.pendingTxns.add(msg.getProposal());
          LOG.debug("Got proposal from request queue!");
        } else if (msg.getType() == MessageType.COMMIT) {
          ZabMessage.Commit commit = request.getMessage().getCommit();
          Zxid zxid = MessageBuilder.fromProtoZxid(commit.getZxid());
          LOG.debug("Received a commit request {}, last {}.",
                    zxid,
                    this.lastDeliveredZxid);
          if (zxid.compareTo(this.lastDeliveredZxid) <= 0) {
            // The leader may send duplicate committed zxids. Avoid delivering
            //duplicate transactions even though transactions are idempotent.
            LOG.debug("{} is duplicated COMMIT message with last {}",
                      zxid,
                      this.lastDeliveredZxid);
            continue;
          }
          int startIdx = 0;
          int endIdx = startIdx;
          for (; endIdx < this.pendingTxns.size(); ++endIdx) {
            ZabMessage.Proposal prop = this.pendingTxns.get(endIdx);
            Transaction txn = MessageBuilder.fromProposal(prop);
            String clientId = prop.getClientId();
            if(zxid.compareTo(txn.getZxid()) < 0) {
              break;
            }
            if (txn.getType() == ProposalType.USER_REQUEST_VALUE) {
              LOG.debug("Delivering transaction {}.", txn.getZxid());
              stateMachine.deliver(txn.getZxid(), txn.getBody(), clientId);
            } else if (txn.getType() == ProposalType.COP_VALUE) {
              LOG.debug("Delivering COP {}.", txn.getZxid());
              ClusterConfiguration cnf =
                ClusterConfiguration.fromByteBuffer(txn.getBody(), "");
              stateMachine.clusterChange(new HashSet<String>(cnf.getPeers()));
              if (!cnf.contains(this.serverId)) {
                // If the new configuration doesn't contain this server, we'll
                // enqueue SHUT_DOWN message to main thread to let it quit.
                LOG.debug("The new configuration doesn't contain {}", serverId);
                Message shutdown = MessageBuilder.buildShutDown();
                this.transport.send(this.serverId,
                                    ByteBuffer.wrap(shutdown.toByteArray()));
              }
            } else {
              LOG.warn("Unknown proposal type.");
            }
            this.lastDeliveredZxid = txn.getZxid();
          }
          // Removes the delivered transactions.
          this.pendingTxns.subList(startIdx, endIdx).clear();
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
    this.commitQueue.add(MessageTuple.REQUEST_OF_DEATH);
    this.ft.get();
  }

  public Zxid getLastDeliveredZxid() {
    return this.lastDeliveredZxid;
  }
}
