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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.HashMap;
import java.util.Map;
import org.apache.zab.proto.ZabMessage;
import org.apache.zab.proto.ZabMessage.Message;
import org.apache.zab.proto.ZabMessage.Message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This processor is used to let clients to convert requests into transaction
 * and hands the transaction to BroadcastProcessor to broadcast to all peers.
 */
public class PreProcessor implements RequestProcessor,
                                     Callable<Void> {

  private final BlockingQueue<MessageTuple> requestQueue =
      new LinkedBlockingQueue<MessageTuple>();

  private static final Logger LOG =
      LoggerFactory.getLogger(PreProcessor.class);

  private final StateMachine stateMachine;

  private final Map<String, PeerHandler> quorumSetOriginal;

  private final Map<String, PeerHandler> quorumSet;

  private final Future<Void> ft;

  private ClusterConfiguration clusterConfig;

  public PreProcessor(StateMachine stateMachine,
                      Map<String, PeerHandler> quorumSet,
                      ClusterConfiguration config) {
    this.stateMachine = stateMachine;
    this.quorumSetOriginal = quorumSet;
    this.quorumSet = new HashMap<String, PeerHandler>(quorumSet);
    this.clusterConfig = config;
    ExecutorService es =
        Executors.newSingleThreadExecutor(DaemonThreadFactory.FACTORY);
    ft = es.submit(this);
    es.shutdown();
  }

  @Override
  public void processRequest(MessageTuple request) {
    this.requestQueue.add(request);
  }

  private void addToQuorumSet(String serverId) {
    PeerHandler ph = this.quorumSetOriginal.get(serverId);
    if (ph != null) {
      this.quorumSet.put(serverId, ph);
    }
  }

  private void removeFromQuorumSet(String serverId) {
    this.quorumSet.remove(serverId);
  }

  @Override
  public Void call() throws Exception {
    LOG.debug("PreProcessor gets started.");
    try {
      while (true) {
        MessageTuple request = this.requestQueue.take();
        if (request == MessageTuple.REQUEST_OF_DEATH) {
          break;
        }
        Message msg = request.getMessage();
        String source = request.getServerId();
        Zxid zxid = request.getZxid();
        if (msg.getType() == MessageType.REQUEST) {
          ZabMessage.Request req = request.getMessage().getRequest();
          String clientId = request.getServerId();
          ByteBuffer bufReq = req.getRequest().asReadOnlyByteBuffer();
          // Invoke the callback to convert the request into transaction.
          ByteBuffer update = this.stateMachine.preprocess(zxid,
                                                           bufReq);
          Transaction txn = new Transaction(zxid, update);
          Message prop = MessageBuilder.buildProposal(txn, clientId);
          for (PeerHandler ph : quorumSet.values()) {
            ph.queueMessage(prop);
          }
        } else if (msg.getType() == MessageType.JOIN) {
          LOG.debug("Got JOIN from {}.", source);
          if (!this.clusterConfig.contains(source)) {
            this.clusterConfig.addPeer(source);
          }
          this.clusterConfig.setVersion(zxid);
          ByteBuffer cop = this.clusterConfig.toByteBuffer();
          Transaction txn = new Transaction(zxid, Transaction.COP, cop);
          Message prop = MessageBuilder.buildProposal(txn);
          // Broadcasts COP.
          for (PeerHandler ph : quorumSet.values()) {
            ph.queueMessage(prop);
          }
          // Adds it to quorum set.
          addToQuorumSet(source);
        } else if (msg.getType() == MessageType.ACK_EPOCH) {
          LOG.debug("Got ACK_EPOCH from {}.", source);
          addToQuorumSet(source);
        } else if (msg.getType() == MessageType.REMOVE) {
          String peerId = msg.getRemove().getServerId();
          LOG.debug("Got REMOVE for {}.", peerId);
          this.clusterConfig.removePeer(peerId);
          this.clusterConfig.setVersion(zxid);
          ByteBuffer cop = this.clusterConfig.toByteBuffer();
          Transaction txn = new Transaction(zxid, Transaction.COP, cop);
          Message prop = MessageBuilder.buildProposal(txn);
          // Broadcasts COP.
          for (PeerHandler ph : quorumSet.values()) {
            ph.queueMessage(prop);
          }
          // Removes it from quorum set.
          removeFromQuorumSet(msg.getDisconnected().getServerId());
        } else if (msg.getType() == MessageType.DISCONNECTED) {
          String peerId = msg.getDisconnected().getServerId();
          LOG.debug("Got DISCONNECTED from {}.", peerId);
          removeFromQuorumSet(peerId);
        } else {
          LOG.warn("Got unexpected Message.");
        }
      }
    } catch (RuntimeException e) {
      LOG.error("Caught exception in PreProcessor!", e);
      throw e;
    }
    LOG.debug("PreProcessor has been shut down.");
    return null;
  }

  @Override
  public void shutdown() throws InterruptedException, ExecutionException {
    this.requestQueue.add(MessageTuple.REQUEST_OF_DEATH);
    this.ft.get();
  }
}

