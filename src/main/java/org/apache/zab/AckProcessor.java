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
import java.util.HashMap;
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
import org.apache.zab.proto.ZabMessage.Message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accepts acknowledgment from peers and broadcasts COMMIT message if there're
 * any committed transactions.
 */
public class AckProcessor implements RequestProcessor,
                                     Callable<Void> {

  private final BlockingQueue<MessageTuple> ackQueue =
      new LinkedBlockingQueue<MessageTuple>();

  private final Map<String, PeerHandler> quorumSetOriginal;

  private final Map<String, PeerHandler> quorumSet;

  private final PersistentState persistence;

  private static final Logger LOG =
      LoggerFactory.getLogger(AckProcessor.class);

  Future<Void> ft;

  /**
   * Last committed zxid sent by AckProcessor. Used to avoid sending duplicated
   * COMMIT message.
   */
  private Zxid lastCommittedZxid;

  public AckProcessor(Map<String, PeerHandler> quorumSet,
                      PersistentState persistence,
                      Zxid lastCommittedZxid) {
    this.quorumSetOriginal = quorumSet;
    this.quorumSet = new HashMap<String, PeerHandler>(quorumSet);
    this.persistence = persistence;
    this.lastCommittedZxid = lastCommittedZxid;
    ExecutorService es =
        Executors.newSingleThreadExecutor(DaemonThreadFactory.FACTORY);
    ft = es.submit(this);
    es.shutdown();
  }

  @Override
  public void processRequest(MessageTuple request) {
    this.ackQueue.add(request);
  }

  @Override
  public Void call() throws Exception {
    LOG.debug("AckProcessor gets started.");
    try {
      while (true) {
        MessageTuple request = ackQueue.take();
        if (request == MessageTuple.REQUEST_OF_DEATH) {
          break;
        }
        Message msg = request.getMessage();
        if (msg.getType() == MessageType.ACK) {
          String source = request.getServerId();
          ZabMessage.Ack ack = request.getMessage().getAck();
          Zxid zxid = MessageBuilder.fromProtoZxid(ack.getZxid());
          this.quorumSet.get(source).setLastAckedZxid(zxid);
          ArrayList<Zxid> zxids = new ArrayList<Zxid>();
          for (PeerHandler ph : quorumSet.values()) {
            LOG.debug("Last zxid of {} is {}",
                      ph.getServerId(),
                      ph.getLastAckedZxid());
            Zxid ackZxid = ph.getLastAckedZxid();
            if (ackZxid != null) {
              // Add acknowledged zxid to zxids only.
              zxids.add(ph.getLastAckedZxid());
            }
          }
          int quorumSize = persistence.getLastSeenConfig().getQuorumSize();
          if (zxids.size() < quorumSize) {
            continue;
          }
          // Sorts the last ACK zxid of each peer to find one transaction which
          // can be committed safely.
          Collections.sort(zxids);
          Zxid zxidCanCommit = zxids.get(zxids.size() - quorumSize);
          LOG.debug("CAN COMMIT : {}", zxidCanCommit);

          if (zxidCanCommit.compareTo(this.lastCommittedZxid) > 0) {
            // Avoid sending duplicated COMMIT message.
            LOG.debug("Will send commit {} to quorumSet.", zxidCanCommit);
            Message commit = MessageBuilder.buildCommit(zxidCanCommit);
            for (PeerHandler ph : quorumSet.values()) {
              ph.queueMessage(commit);
            }
            this.lastCommittedZxid = zxidCanCommit;
          }
        } else if (msg.getType() == MessageType.ADD_FOLLOWER) {
          String followerId = msg.getAddFollower().getFollowerId();
          LOG.debug("Got ADD_FOLLOWER for {}.", followerId);
          PeerHandler ph = this.quorumSetOriginal.get(followerId);
          if (ph != null) {
            this.quorumSet.put(followerId, ph);
          }
        } else if (msg.getType() == MessageType.REMOVE_FOLLOWER) {
          String followerId = msg.getRemoveFollower().getFollowerId();
          LOG.debug("Got REMOVE_FOLLOWER for {}.", followerId);
          this.quorumSet.remove(followerId);
        } else {
          LOG.warn("Got unexpected message.");
        }
      }
    } catch (RuntimeException e) {
      LOG.error("Caught exception in AckProcessor!", e);
      throw e;
    }
    LOG.debug("AckProcesser has been shut down.");
    return null;
  }

  @Override
  public void shutdown() throws InterruptedException, ExecutionException {
    this.ackQueue.add(MessageTuple.REQUEST_OF_DEATH);
    this.ft.get();
  }
}
