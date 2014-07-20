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

  private final BlockingQueue<Request> requestQueue =
      new LinkedBlockingQueue<Request>();

  private static final Logger LOG =
      LoggerFactory.getLogger(PreProcessor.class);

  private final StateMachine stateMachine;

  // Last proposed Zxid from this processor. It starts from <f.a, -1>
  private Zxid lastProposedZxid;

  private final String leaderId;

  private final Map<String, PeerHandler> quorumSet;

  Future<Void> ft;


  public PreProcessor(StateMachine stateMachine,
                      int currentEpoch,
                      Map<String, PeerHandler> quorumSet,
                      String leaderId) {
    this.stateMachine = stateMachine;
    this.lastProposedZxid = new Zxid(currentEpoch, -1);
    this.leaderId = leaderId;
    this.quorumSet = quorumSet;
    ExecutorService es =
        Executors.newSingleThreadExecutor(DaemonThreadFactory.FACTORY);
    ft = es.submit(this);
    es.shutdown();
  }

  @Override
  public void processRequest(Request request) {
    this.requestQueue.add(request);
  }

  @Override
  public Void call() throws Exception {
    LOG.debug("PreProcessor gets started.");
    try {
      while (true) {
        Request request = this.requestQueue.take();
        if (request == Request.REQUEST_OF_DEATH) {
          break;
        }
        if (request.getMessage().getType() == MessageType.FLUSH_PREPROCESSOR) {
          LOG.debug("Got FLUSH_PREPROCESSOR msg.");
          this.quorumSet.get(this.leaderId).queueMessage(request.getMessage());
          continue;
        }
        lastProposedZxid = new Zxid(lastProposedZxid.getEpoch(),
                                    lastProposedZxid.getXid() + 1);
        ZabMessage.Request req = request.getMessage().getRequest();
        String clientId = request.getServerId();
        ByteBuffer bufReq = req.getRequest().asReadOnlyByteBuffer();
        // Invoke the callback to convert the request into transaction.
        ByteBuffer update = this.stateMachine.preprocess(lastProposedZxid,
                                                         bufReq);
        Message prop = MessageBuilder
                       .buildProposal(new Transaction(lastProposedZxid, update),
                                      clientId);
        for (PeerHandler ph : quorumSet.values()) {
          ph.queueMessage(prop);
        }
      }
    } catch (Exception e) {
      LOG.error("Caught exception in PreProcessor!", e);
      throw e;
    }
    LOG.debug("PreProcessor has been shut down.");
    return null;
  }

  @Override
  public void shutdown() throws InterruptedException, ExecutionException {
    this.requestQueue.add(Request.REQUEST_OF_DEATH);
    this.ft.get();
  }
}

