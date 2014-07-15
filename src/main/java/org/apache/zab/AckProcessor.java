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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.zab.proto.ZabMessage;
import org.apache.zab.proto.ZabMessage.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accepts acknowledgment from peers and broadcasts COMMIT message if there're
 * any committed transactions.
 */
public class AckProcessor implements RequestProcessor,
                                        Callable<Void> {

  private final BlockingQueue<Request> ackQueue =
      new LinkedBlockingQueue<Request>();

  private final Map<String, PeerHandler> quorumSet;

  private final int quorumSize;

  private static final Logger LOG =
      LoggerFactory.getLogger(AckProcessor.class);

  Future<Void> ft;

  public AckProcessor(Map<String, PeerHandler> quorumSet,
                         int quorumSize) {
    this.quorumSet = quorumSet;
    this.quorumSize = quorumSize;
    ExecutorService es =
        Executors.newSingleThreadExecutor(DaemonThreadFactory.FACTORY);
    ft = es.submit(this);
    es.shutdown();
  }

  @Override
  public void processRequest(Request request) {
    this.ackQueue.add(request);
  }

  @Override
  public Void call() throws Exception {
    LOG.debug("AckProcessor gets started.");
    try {
      while (true) {
        Request request = ackQueue.take();

        if (request == Request.REQUEST_OF_DEATH) {
          break;
        }

        String source = request.getServerId();
        ZabMessage.Ack ack = request.getMessage().getAck();
        Zxid zxid = MessageBuilder.fromProtoZxid(ack.getZxid());
        this.quorumSet.get(source).setLastAckedZxid(zxid);
        ArrayList<Zxid> zxids = new ArrayList<Zxid>();
        for (PeerHandler ph : quorumSet.values()) {
          LOG.debug("Last zxid of {} is {}",
                    ph.getServerId(),
                    ph.getLastAckedZxid());
          zxids.add(ph.getLastAckedZxid());
        }
        // Sorts the last ACK zxid of each peer to find one transaction which
        // can be committed safely.
        Collections.sort(zxids);
        Zxid zxidCanCommit = zxids.get(zxids.size() - this.quorumSize);
        LOG.debug("CAN COMMIT : {}", zxidCanCommit);
        Message commit = MessageBuilder.buildCommit(zxidCanCommit);
        for (PeerHandler ph : quorumSet.values()) {
          Zxid lastCommittedZxid = ph.getLastCommittedZxid();
          // Sends the COMMIT message only if the peer has not be sent the same
          // COMMIT message before.
          if (lastCommittedZxid == null ||
              lastCommittedZxid.compareTo(zxidCanCommit) < 0) {
            LOG.debug("Will send commit {} to {}.", ph.getServerId());
            ph.queueMessage(commit);
            ph.setLastCommittedZxid(zxidCanCommit);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Caught exception in AckProcessor!", e);
      throw e;
    }
    LOG.debug("AckProcesser has been shut down.");
    return null;
  }

  @Override
  public void shutdown() throws InterruptedException, ExecutionException {
    this.ackQueue.add(Request.REQUEST_OF_DEATH);
    this.ft.get();
  }
}
