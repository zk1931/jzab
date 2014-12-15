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
import com.github.zk1931.jzab.proto.ZabMessage;
import com.github.zk1931.jzab.proto.ZabMessage.Message;
import com.github.zk1931.jzab.proto.ZabMessage.Message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accepts acknowledgment from peers and broadcasts COMMIT message if there're
 * any committed transactions.
 */
class AckProcessor implements RequestProcessor, Callable<Void> {

  private final BlockingQueue<MessageTuple> ackQueue =
      new LinkedBlockingQueue<MessageTuple>();

  /**
   * The quorum set in main thread.
   */
  private final Map<String, PeerHandler> quorumMapOriginal;

  /**
   * The quorum set for AckProcessor.
   */
  private final Map<String, PeerHandler> quorumMap;

  /**
   * Current cluster configuration.
   */
  private ClusterConfiguration clusterConfig;

  /**
   * The pending configuration which has not been committed yet.
   */
  private ClusterConfiguration pendingConfig;

  private static final Logger LOG =
      LoggerFactory.getLogger(AckProcessor.class);

  Future<Void> ft;

  public AckProcessor(Map<String, PeerHandler> quorumMap,
                      ClusterConfiguration cnf) {
    this.quorumMapOriginal = quorumMap;
    this.quorumMap = new HashMap<String, PeerHandler>(quorumMap);
    this.clusterConfig = cnf.clone();
    ExecutorService es =
        Executors.newSingleThreadExecutor(DaemonThreadFactory.FACTORY);
    ft = es.submit(this);
    es.shutdown();
  }

  @Override
  public void processRequest(MessageTuple request) {
    this.ackQueue.add(request);
  }

  // Given a cluster configuration and the quorum set, find out the last zxid of
  // the transactions which could be committed for the given cluster
  // configuration.
  private Zxid getCommittedZxid(ClusterConfiguration cnf) {
    ArrayList<Zxid> zxids = new ArrayList<Zxid>();
    LOG.debug("Getting zxid can be committd for cluster configuration {}",
              cnf.getVersion());
    for (PeerHandler ph : quorumMap.values()) {
      Zxid ackZxid = ph.getLastAckedZxid();
      if (cnf.contains(ph.getServerId())) {
        // Only consider the peer who is in the given configuration.
        if (ackZxid != null) {
          // Ignores those who haven't acknowledged.
          zxids.add(ackZxid);
        }
        LOG.debug(" - {}'s last acked zxid {}", ph.getServerId(), ackZxid);
      }
    }
    int quorumSize = cnf.getQuorumSize();
    if (quorumSize == 0) {
      // In one case, there's only one server in cluster, and the server is
      // removed. Commit it directly.
      return cnf.getVersion();
    }
    if (zxids.size() < quorumSize) {
      // It's impossible to be committed.
      return Zxid.ZXID_NOT_EXIST;
    }
    // Sorts the last ACK zxid of each peer to find one transaction which
    // can be committed safely.
    Collections.sort(zxids);
    return zxids.get(zxids.size() - quorumSize);
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
        String source = request.getServerId();
        if (msg.getType() == MessageType.ACK) {
          ZabMessage.Ack ack = request.getMessage().getAck();
          Zxid zxid = MessageBuilder.fromProtoZxid(ack.getZxid());
          LOG.debug("Got ACK {} from {}", zxid, source);
          this.quorumMap.get(source).setLastAckedZxid(zxid);
          // The zxid of last transaction which could be committed.
          Zxid zxidCanCommit = null;
          // Check if there's a pending reconfiguration.
          if (this.pendingConfig != null) {
            // Find out the last transaction which can be committed for pending
            // configuration.
            zxidCanCommit = getCommittedZxid(this.pendingConfig);
            LOG.debug("Zxid can be committed for pending configuration is {}.",
                      zxidCanCommit);
            if (zxidCanCommit.compareTo(pendingConfig.getVersion()) >= 0) {
              // The pending configuration is just committed, make it becomes
              // current configuration.
              LOG.debug("Pending configuration {} is committed, turn it into" +
                  " current configuration.", pendingConfig.getVersion());
              this.clusterConfig = this.pendingConfig;
              this.pendingConfig = null;
            } else {
              // Still hasn't been committed yet.
              zxidCanCommit = null;
            }
          }
          if (zxidCanCommit == null) {
            // Find out the last transaction which can be committed for current
            // configuration.
            zxidCanCommit = getCommittedZxid(this.clusterConfig);
            if (pendingConfig != null &&
                zxidCanCommit.compareTo(pendingConfig.getVersion()) >= 0) {
              // We still shouldn't commit any transaction after COP if they
              // are just acknowledged by a quorum of old configuration.
              Zxid version = pendingConfig.getVersion();
              // Then commit the transactions up to the one before COP.
              if (version.getXid() == 0) {
                // Means the COP is the first transaction in this epoch, no
                // transactions before COP needs to be committed.
                continue;
              } else {
                // We can commit the transaction up to the one before COP.
                zxidCanCommit =
                  new Zxid(version.getEpoch(), version.getXid() - 1);
              }
            }
            LOG.debug("Zxid can be committed for current configuration is {}",
                      zxidCanCommit);
          }
          LOG.debug("Can COMMIT : {}", zxidCanCommit);
          for (PeerHandler ph : quorumMap.values()) {
            if (ph.getLastAckedZxid() == null) {
              // Means the server hasn't acked any proposal.
              continue;
            }
            Zxid zxidCommit = zxidCanCommit;
            if (zxidCommit.compareTo(ph.getLastAckedZxid()) > 0) {
              // We shouldn't send COMMIT with zxid higher than what the peer
              // acknowledged.
              zxidCommit = ph.getLastAckedZxid();
            }
            if (zxidCommit.compareTo(ph.getLastCommittedZxid()) > 0) {
              // Avoids sending duplicate transactions even the transactions
              // are idempotent.
              Message commit = MessageBuilder.buildCommit(zxidCommit);
              ph.queueMessage(commit);
              ph.setLastCommittedZxid(zxidCommit);
            }
          }
        } else if (msg.getType() == MessageType.JOIN ||
                   msg.getType() == MessageType.ACK_EPOCH) {
          PeerHandler ph = quorumMapOriginal.get(source);
          if (ph != null) {
            this.quorumMap.put(source, ph);
          }
          if (msg.getType() == MessageType.JOIN) {
            LOG.debug("Got JOIN({}) from {}", request.getZxid(), source);
            if (pendingConfig != null) {
              LOG.error("A pending reconfig is still in progress, a bug?");
              throw new RuntimeException("Still has pending reconfiguration");
            }
            this.pendingConfig = this.clusterConfig.clone();
            this.pendingConfig.addPeer(source);
            // Update zxid for this reconfiguration.
            this.pendingConfig.setVersion(request.getZxid());
          }
        } else if (msg.getType() == MessageType.DISCONNECTED) {
          String peerId = msg.getDisconnected().getServerId();
          LOG.debug("Got DISCONNECTED from {}.", peerId);
          this.quorumMap.remove(peerId);
        } else if (msg.getType() == MessageType.REMOVE) {
          String serverId = msg.getRemove().getServerId();
          LOG.debug("Got REMOVE({})for {}", request.getZxid(), serverId);
          if (pendingConfig != null) {
            LOG.error("A pending reconfig is still in progress, a bug?");
            throw new RuntimeException("Still has pending reconfiguration");
          }
          this.pendingConfig = this.clusterConfig.clone();
          this.pendingConfig.removePeer(serverId);
          // Update zxid for this reconfiguration.
          this.pendingConfig.setVersion(request.getZxid());
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
