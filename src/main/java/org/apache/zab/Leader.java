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

import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.zab.proto.ZabMessage;
import org.apache.zab.proto.ZabMessage.Message;
import org.apache.zab.proto.ZabMessage.Message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Leader.
 */
public class Leader extends Participant {

  private final Map<String, PeerHandler> quorumSet =
    new ConcurrentHashMap<String, PeerHandler>();

  // Stores all the new joined/recovered servers who are waiting for
  // synchronization ends.
  private final Map<String, PeerHandler> pendingPeers =
    new HashMap<String, PeerHandler>();

  /**
   * Last proposed Zxid of the leader.
   */
  private Zxid lastProposedZxid;

  /**
   * Last acknowledged zxid of the leader.
   */
  private Zxid lastAckedZxid;

  /**
   * Last committed zxid of the leader.
   */
  private Zxid lastCommittedZxid;

  /**
   * The established epoch for this leader.
   */
  private int establishedEpoch = -1;

  /**
   * The zxid for pending COP, or null if there's no pending COP.
   */
  private Zxid pendingCopZxid = null;

  private static final Logger LOG = LoggerFactory.getLogger(Leader.class);

  public Leader(ParticipantState participantState,
                StateMachine stateMachine,
                ZabConfig config) {
    super(participantState, stateMachine, config);
    this.electedLeader = participantState.getServerId();
    MDC.put("state", "leading");
  }

  /**
   * Gets a message from the queue.
   *
   * @return a message tuple contains the message and its source.
   * @throws TimeoutException in case of timeout.
   * @throws InterruptedException it's interrupted.
   */
  @Override
  protected MessageTuple getMessage()
      throws TimeoutException, InterruptedException {
    while (true) {
      MessageTuple tuple = messageQueue.poll(config.getTimeout(),
                                             TimeUnit.MILLISECONDS);
      if (tuple == null) {
        // Timeout.
        throw new TimeoutException("Timeout while waiting for the message.");
      } else if (tuple == MessageTuple.GO_BACK) {
        // Goes back to leader election.
        throw new BackToElectionException();
      } else if (tuple.getMessage().getType() == MessageType.DISCONNECTED) {
        // Got DISCONNECTED message enqueued by onDisconnected callback.
        Message msg = tuple.getMessage();
        String peerId = msg.getDisconnected().getServerId();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Got message {}.",
                    TextFormat.shortDebugString(msg));
        }
        if (this.quorumSet.containsKey(peerId)) {
          if (!this.isBroadcasting) {
            // If you lost someone in your quorumSet before broadcasting
            // phase, you are for sure not have a quorum of followers, just go
            // back to leader election. The clearance of the transport happens
            // in the exception handlers of lead/join function.
            LOG.debug("Lost follower {} in the quorumSet.", peerId);
            throw new BackToElectionException();
          } else {
            // Lost someone who is in the quorumSet in broadcasting phase,
            // return this message to caller and let it handles the
            // disconnection.
            LOG.debug("Lost follower {} in the quorumSet.", peerId);
            return tuple;
          }
        } else {
          // Just lost someone you don't care, clear the transport so it can
          // join in in later time.
          LOG.debug("Lost follower {} outside quorumSet.", peerId);
          this.transport.clear(peerId);
        }
      } else {
        return tuple;
      }
    }
  }

  @Override
  protected void changePhase(Phase phase) throws IOException {
    if (phase == Phase.DISCOVERING) {
      MDC.put("phase", "discovering");
      if (stateChangeCallback != null) {
        stateChangeCallback.leaderDiscovering(this.serverId);
      }
      if (failCallback != null) {
        failCallback.leaderDiscovering();
      }
    } else if (phase == Phase.SYNCHRONIZING) {
      MDC.put("phase", "synchronizing");
      if (stateChangeCallback != null) {
        stateChangeCallback.leaderSynchronizing(persistence.getProposedEpoch());
      }
      if (failCallback != null) {
        failCallback.leaderSynchronizing();
      }
    } else if (phase == Phase.BROADCASTING) {
      MDC.put("phase", "broadcasting");
      this.isBroadcasting = true;
      if (failCallback != null) {
        failCallback.leaderBroadcasting();
      }
      if (stateChangeCallback != null) {
        stateChangeCallback.leaderBroadcasting(persistence.getAckEpoch(),
                                               getAllTxns(),
                                               persistence.getLastSeenConfig());
      }
    }
  }

  /**
   * Starts from joining leader itself.
   *
   * @param peer should be as same as serverId of leader.
   * @throws Exception in case something goes wrong.
   */
  @Override
  public void join(String peer) throws Exception {
    try {
      // Initializes the persistent variables.
      List<String> peers = new ArrayList<String>();
      peers.add(this.serverId);
      ClusterConfiguration cnf =
        new ClusterConfiguration(Zxid.ZXID_NOT_EXIST,
                                 peers,
                                 this.serverId);
      persistence.setLastSeenConfig(cnf);
      persistence.setProposedEpoch(0);
      persistence.setAckEpoch(0);

      /* -- Broadcasting phase -- */
      changePhase(Phase.BROADCASTING);
      broadcasting();
    } catch (InterruptedException e) {
      LOG.debug("Participant is canceled by user.");
      throw e;
    } catch (TimeoutException e) {
      LOG.debug("Didn't hear message from peers for {} milliseconds. Going"
                + " back to leader election.",
                this.config.getTimeout());
    } catch (BackToElectionException e) {
      LOG.debug("Got GO_BACK message from queue, going back to electing.");
    } catch (LeftCluster e) {
      LOG.debug("Exit running : {}", e.getMessage());
      throw e;
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      throw e;
    } finally {
      for (PeerHandler ph : this.quorumSet.values()) {
        ph.shutdown();
        this.quorumSet.remove(ph.getServerId());
      }
    }
  }

  public void lead() throws Exception {
    try {
      /* -- Discovering phase -- */
      changePhase(Phase.DISCOVERING);
      waitProposedEpochFromQuorum();
      proposeNewEpoch();
      waitEpochAckFromQuorum();
      LOG.debug("Established new epoch {}",
                persistence.getProposedEpoch());
      // Finds one who has the "best" history.
      String peerId = selectSyncHistoryOwner();
      LOG.debug("Chose {} to pull its history.", peerId);

      /* -- Synchronizing phase -- */
      changePhase(Phase.SYNCHRONIZING);
      if (!peerId.equals(this.serverId)) {
        // Pulls history from the follower.
        synchronizeFromFollower(peerId);
      }
      // Updates ACK EPOCH of leader.
      persistence.setAckEpoch(persistence.getProposedEpoch());
      beginSynchronizing();
      waitNewLeaderAckFromQuorum();

      // Broadcasts commit message.
      Message commit = MessageBuilder
                       .buildCommit(persistence.getLog().getLatestZxid());
      broadcast(this.quorumSet.keySet().iterator(), commit);
      // See if it can be restored from the snapshot file.
      restoreFromSnapshot();
      // Delivers all the txns in log before entering broadcasting phase.
      deliverUndeliveredTxns();

      /* -- Broadcasting phase -- */
      changePhase(Phase.BROADCASTING);
      for (PeerHandler ph : this.quorumSet.values()) {
        ph.startBroadcastingTask();
      }
      broadcasting();
    } catch (InterruptedException e) {
      LOG.debug("Participant is canceled by user.");
      throw e;
    } catch (TimeoutException e) {
      LOG.debug("Didn't hear message from peers for {} milliseconds. Going"
                + " back to leader election.",
                this.config.getTimeout());
    } catch (BackToElectionException e) {
      LOG.debug("Got GO_BACK message from queue, going back to electing.");
    } catch (QuorumZab.SimulatedException e) {
      LOG.debug("Got SimulatedException, go back to leader election.");
    } catch (LeftCluster e) {
      LOG.debug("Exit running : {}", e.getMessage());
      throw e;
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      throw e;
    } finally {
      for (PeerHandler ph : this.quorumSet.values()) {
        ph.shutdown();
        this.quorumSet.remove(ph.getServerId());
      }
    }
  }

  /**
   * Gets the minimal quorum size.
   *
   * @return the minimal quorum size
   * @throws IOException in case of IO failures.
   */
  public int getQuorumSize() throws IOException {
    return persistence.getLastSeenConfig().getQuorumSize();
  }

  /**
   * Waits until receives the CEPOCH message from the quorum.
   *
   * @throws InterruptedException if anything wrong happens.
   * @throws TimeoutException in case of timeout.
   */
  void waitProposedEpochFromQuorum()
      throws InterruptedException, TimeoutException, IOException {
    ClusterConfiguration currentConfig = persistence.getLastSeenConfig();
    int acknowledgedEpoch = persistence.getAckEpoch();
    // Waits PROPOED_EPOCH from a quorum of peers in current configuraion.
    while (this.quorumSet.size() < getQuorumSize() - 1) {
      MessageTuple tuple = getExpectedMessage(MessageType.PROPOSED_EPOCH, null);
      Message msg = tuple.getMessage();
      String source = tuple.getServerId();
      ZabMessage.ProposedEpoch epoch = msg.getProposedEpoch();
      ClusterConfiguration peerConfig =
        ClusterConfiguration.fromProto(epoch.getConfig(), source);
      int peerProposedEpoch = epoch.getProposedEpoch();
      int peerAckedEpoch = epoch.getCurrentEpoch();
      Zxid peerVersion = peerConfig.getVersion();
      Zxid selfVersion = currentConfig.getVersion();
      // If the peer's config version doesn't match leader's config version,
      // we'll check if the peer has the more likely "correct" configuration.
      if (!peerVersion.equals(selfVersion)) {
        LOG.debug("{}'s config version {} is different with leader's {}",
                  peerVersion, selfVersion);
        if (peerAckedEpoch > acknowledgedEpoch ||
            (peerAckedEpoch == acknowledgedEpoch &&
             peerVersion.compareTo(selfVersion) > 0)) {
          LOG.debug("{} probably has right configuration, go back to "
              + "leader election.",
              source);
          // TODO : current we just go back to leader election, probably we want
          // to select the peer as leader.
          throw new BackToElectionException();
        }
      }
      // Rejects peer who is not in the current configuration.
      if (!currentConfig.contains(source)) {
        LOG.debug("Current configuration doesn't contain {}, ignores it.",
                  source);
        continue;
      }
      if (this.quorumSet.containsKey(source)) {
        throw new RuntimeException("Quorum set has already contained "
            + source + ", probably a bug?");
      }
      PeerHandler ph = new PeerHandler(source, transport,
                                       config.getTimeout()/3);
      ph.setLastProposedEpoch(peerProposedEpoch);
      this.quorumSet.put(source, ph);
    }
    LOG.debug("Got proposed epoch from a quorum.");
  }

  /**
   * Finds an epoch number which is higher than any proposed epoch in quorum
   * set and propose the epoch to them.
   *
   * @throws IOException in case of IO failure.
   */
  void proposeNewEpoch()
      throws IOException {
    List<Integer> epochs = new ArrayList<Integer>();
    // Puts leader's last received proposed epoch in list.
    epochs.add(persistence.getProposedEpoch());
    for (PeerHandler ph : this.quorumSet.values()) {
      epochs.add(ph.getLastProposedEpoch());
    }
    int newEpoch = Collections.max(epochs) + 1;
    // Updates leader's last proposed epoch.
    persistence.setProposedEpoch(newEpoch);
    LOG.debug("Begins proposing new epoch {}", newEpoch);
    // Sends new epoch message to quorum.
    broadcast(this.quorumSet.keySet().iterator(),
              MessageBuilder.buildNewEpochMessage(newEpoch));
  }

  /**
   * Broadcasts the message to all the peers.
   *
   * @param peers the destination of peers.
   * @param message the message to be broadcasted.
   */
  void broadcast(Iterator<String> peers, Message message) {
    transport.broadcast(peers, message);
  }

  /**
   * Waits until the new epoch is established.
   *
   * @throws InterruptedException if anything wrong happens.
   * @throws TimeoutException in case of timeout.
   */
  void waitEpochAckFromQuorum()
      throws InterruptedException, TimeoutException {
    int ackCount = 0;
    // Waits the Ack from all other peers in the quorum set.
    while (ackCount < this.quorumSet.size()) {
      MessageTuple tuple = getExpectedMessage(MessageType.ACK_EPOCH, null);
      Message msg = tuple.getMessage();
      String source = tuple.getServerId();
      if (!this.quorumSet.containsKey(source)) {
        LOG.warn("The Epoch ACK comes from {} who is not in quorum set, "
                 + "possibly from previous epoch?",
                 source);
        continue;
      }
      ackCount++;
      ZabMessage.AckEpoch ackEpoch = msg.getAckEpoch();
      ZabMessage.Zxid zxid = ackEpoch.getLastZxid();
      // Updates follower's f.a and lastZxid.
      PeerHandler ph = this.quorumSet.get(source);
      ph.setLastAckedEpoch(ackEpoch.getAcknowledgedEpoch());
      ph.setLastZxid(MessageBuilder.fromProtoZxid(zxid));
    }
    LOG.debug("Received ACKs from the quorum set of size {}.",
              this.quorumSet.size() + 1);
  }

  /**
   * Finds a server who has the largest acknowledged epoch and longest
   * history.
   *
   * @return the id of the server
   * @throws IOException
   */
  String selectSyncHistoryOwner()
      throws IOException {
    // L.1.2 Select the history of a follwer f to be the initial history
    // of the new epoch. Follwer f is such that for every f' in the quorum,
    // f'.a < f.a or (f'.a == f.a && f'.zxid <= f.zxid).
    int ackEpoch = persistence.getAckEpoch();
    Zxid zxid = persistence.getLog().getLatestZxid();
    String peerId = this.serverId;
    Iterator<Map.Entry<String, PeerHandler>> iter;
    iter = this.quorumSet.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, PeerHandler> entry = iter.next();
      int fEpoch = entry.getValue().getLastAckedEpoch();
      Zxid fZxid = entry.getValue().getLastZxid();
      if (fEpoch > ackEpoch ||
          (fEpoch == ackEpoch && fZxid.compareTo(zxid) > 0)) {
        ackEpoch = fEpoch;
        zxid = fZxid;
        peerId = entry.getKey();
      }
    }
    LOG.debug("{} has largest acknowledged epoch {} and longest history {}",
              peerId, ackEpoch, zxid);
    if (this.stateChangeCallback != null) {
      this.stateChangeCallback.initialHistoryOwner(peerId, ackEpoch, zxid);
    }
    return peerId;
  }

  /**
   * Pulls the history from the server who has the "best" history.
   *
   * @param peerId the id of the server whose history is selected.
   */
  void synchronizeFromFollower(String peerId)
      throws IOException, TimeoutException, InterruptedException {
    LOG.debug("Begins synchronizing from follower {}.", peerId);
    Zxid lastZxid = persistence.getLog().getLatestZxid();
    Message pullTxn = MessageBuilder.buildPullTxnReq(lastZxid);
    LOG.debug("Last zxid of {} is {}", this.serverId, lastZxid);
    sendMessage(peerId, pullTxn);
    // Waits until the synchronization is finished.
    waitForSync(peerId);
  }

  /**
   * Waits for synchronization to followers complete.
   *
   * @throws TimeoutException in case of timeout.
   * @throws InterruptedException in case of interrupt.
   */
  void waitNewLeaderAckFromQuorum()
      throws TimeoutException, InterruptedException, IOException {
    LOG.debug("Waiting for synchronization to followers complete.");
    int completeCount = 0;
    Zxid lastZxid = persistence.getLog().getLatestZxid();
    while (completeCount < this.quorumSet.size()) {
      MessageTuple tuple = getExpectedMessage(MessageType.ACK, null);
      ZabMessage.Ack ack = tuple.getMessage().getAck();
      String source =tuple.getServerId();
      Zxid zxid = MessageBuilder.fromProtoZxid(ack.getZxid());
      if (!this.quorumSet.containsKey(source)) {
        LOG.warn("Quorum set doesn't contain {}, a bug?", source);
        continue;
      }
      if (zxid.compareTo(lastZxid) != 0) {
        LOG.error("The follower {} is not correctly synchronized.", source);
        throw new RuntimeException("The synchronized follower's last zxid"
            + "doesn't match last zxid of current leader.");
      }
      PeerHandler ph = this.quorumSet.get(source);
      ph.setLastAckedZxid(zxid);
      completeCount++;
    }
  }

  /**
   * Starts synchronizing peers in background threads.
   *
   * @param es the ExecutorService used to running synchronizing tasks.
   * @throws IOException in case of IO failure.
   */
  void beginSynchronizing() throws IOException {
    // Synchronization is performed in other threads.
    Zxid lastZxid = persistence.getLog().getLatestZxid();
    ClusterConfiguration clusterConfig = persistence.getLastSeenConfig();
    int proposedEpoch = persistence.getProposedEpoch();
    for (PeerHandler ph : this.quorumSet.values()) {
      ph.setSyncTask(new SyncPeerTask(ph.getServerId(), ph.getLastZxid(),
                                      lastZxid, clusterConfig),
                     proposedEpoch);
      ph.startSynchronizingTask();
    }
  }

  /**
   * Entering broadcasting phase, leader broadcasts proposal to
   * followers.
   *
   * @throws InterruptedException if it's interrupted.
   * @throws TimeoutException in case of timeout.
   * @throws IOException in case of IO failure.
   * @throws ExecutionException in case of exception from executors.
   */
  void broadcasting()
      throws TimeoutException, InterruptedException, IOException,
      ExecutionException {
    Zxid lastZxid = persistence.getLog().getLatestZxid();
    this.establishedEpoch = persistence.getAckEpoch();
    // Add leader itself to quorumSet.
    PeerHandler lh = new PeerHandler(serverId, transport,
                                     config.getTimeout()/3);
    lh.setLastAckedZxid(lastZxid);
    lh.startBroadcastingTask();
    quorumSet.put(this.serverId, lh);
    // Gets the initial configuration at the beginning of broadcasting.
    ClusterConfiguration clusterConfig = persistence.getLastSeenConfig();
    PreProcessor preProcessor =
      new PreProcessor(stateMachine, quorumSet, clusterConfig.clone());
    AckProcessor ackProcessor =
      new AckProcessor(quorumSet, clusterConfig.clone(), lastZxid);
    SyncProposalProcessor syncProcessor =
        new SyncProposalProcessor(persistence, transport, SYNC_MAX_BATCH_SIZE);
    CommitProcessor commitProcessor =
        new CommitProcessor(stateMachine, lastDeliveredZxid, serverId,
                            transport, new HashSet<String>(quorumSet.keySet()),
                            clusterConfig, electedLeader);
    SnapshotProcessor snapProcessor =
      new SnapshotProcessor(stateMachine, persistence);
    // First time notifies the client active members and cluster configuration.
    stateMachine.leading(new HashSet<String>(quorumSet.keySet()),
                         new HashSet<String>(clusterConfig.getPeers()));
    // Starts thread to process request in request queue.
    SendRequestTask sendTask = new SendRequestTask(this.serverId);
    this.lastCommittedZxid = lastZxid;
    this.lastProposedZxid = lastZxid;
    this.lastAckedZxid = lastZxid;
    try {
      while (this.quorumSet.size() >= clusterConfig.getQuorumSize()) {
        MessageTuple tuple = getMessage();
        Message msg = tuple.getMessage();
        String source = tuple.getServerId();
        if (msg.getType() == MessageType.PROPOSED_EPOCH) {
          LOG.debug("Got PROPOSED_EPOCH from {}.", source);
          ClusterConfiguration cnf = persistence.getLastSeenConfig();
          if (!cnf.contains(source)) {
            // Only allows servers who are in the current config to join.
            LOG.warn("Got PROPOSED_EPOCH from {} who is not in config, "
                + "ignores it.", source);
            continue;
          }
          Message newEpoch =
            MessageBuilder.buildNewEpochMessage(this.establishedEpoch);
          sendMessage(source, newEpoch);
        } else if (msg.getType() == MessageType.ACK_EPOCH) {
          LOG.debug("Got ACK_EPOCH from {}", source);
          // We'll synchronize the follower up to last proposed zxid.
          tuple.setZxid(lastProposedZxid);
          onAckEpoch(tuple, preProcessor, ackProcessor, commitProcessor);
        } else if (msg.getType() == MessageType.QUERY_LEADER) {
          LOG.debug("Got QUERY_LEADER from {}", source);
          Message reply = MessageBuilder.buildQueryReply(this.serverId);
          sendMessage(source, reply);
        } else if (msg.getType() == MessageType.JOIN) {
          LOG.debug("Got JOIN from {}", source);
          if (pendingCopZxid != null) {
            LOG.warn("There's a pending reconfiguration still in progress.");
            continue;
          }
          pendingCopZxid = getNextProposedZxid();
          tuple.setZxid(pendingCopZxid);
          onJoin(tuple, preProcessor, ackProcessor, commitProcessor,
                 clusterConfig);
        } else {
          // In broadcasting phase, the only expected messages come outside
          // the quorum set is PROPOSED_EPOCH, ACK_EPOCH, QUERY_LEADER and JOIN.
          if (!this.quorumSet.containsKey(source)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got message {} from {} outside quorum.",
                        TextFormat.shortDebugString(msg),
                        source);
            }
            continue;
          }
          if (msg.getType() != MessageType.HEARTBEAT && LOG.isDebugEnabled()) {
            LOG.debug("Got message {} from {}",
                      TextFormat.shortDebugString(msg), source);
          }
          if (msg.getType() == MessageType.ACK) {
            onAck(tuple, ackProcessor);
          } else if (msg.getType() == MessageType.REQUEST) {
            Zxid proposedZxid = getNextProposedZxid();
            // Updates last proposed zxid for this peer. The FLUSH
            // message needs this zxid to determine when it's safe
            // to deliver the FLUSH request back to user.
            quorumSet.get(source).setLastProposedZxid(proposedZxid);
            tuple.setZxid(proposedZxid);
            preProcessor.processRequest(tuple);
          } else if (msg.getType() == MessageType.FLUSH_REQ) {
            onFlushReq(tuple);
          } else if (msg.getType() == MessageType.FLUSH) {
            onFlush(tuple, commitProcessor);
          } else if (msg.getType() == MessageType.HEARTBEAT) {
            LOG.trace("Got HEARTBEAT replies from {}", source);
          } else if (msg.getType() == MessageType.PROPOSAL) {
            syncProcessor.processRequest(tuple);
            commitProcessor.processRequest(tuple);
          } else if (msg.getType() == MessageType.COMMIT) {
            onCommit(tuple, commitProcessor);
          } else if (msg.getType() == MessageType.DISCONNECTED) {
            onDisconnected(tuple, preProcessor, ackProcessor, commitProcessor);
          } else if (msg.getType() == MessageType.REMOVE) {
            if (pendingCopZxid != null) {
              LOG.warn("There's a pending reconfiguration still in progress.");
              continue;
            }
            pendingCopZxid = getNextProposedZxid();
            tuple.setZxid(pendingCopZxid);
            onRemove(tuple, preProcessor, ackProcessor, clusterConfig);
          } else if (msg.getType() == MessageType.SHUT_DOWN) {
            throw new LeftCluster("Left cluster");
          } else if (msg.getType() == MessageType.DELIVERED) {
            onDelivered(msg, snapProcessor);
          } else {
            if (LOG.isWarnEnabled()) {
              LOG.warn("Unexpected messgae : {} from {}",
                       TextFormat.shortDebugString(msg),
                       source);
            }
          }
          PeerHandler ph = quorumSet.get(source);
          if (ph != null) {
            ph.updateHeartbeatTime();
          }
          checkFollowerLiveness();
        }
      }
      LOG.debug("Detects the size of the ensemble is less than the"
          + "quorum size {}, goes back to electing phase.",
          getQuorumSize());
    } finally {
      sendTask.shutdown();
      ackProcessor.shutdown();
      preProcessor.shutdown();
      commitProcessor.shutdown();
      syncProcessor.shutdown();
      snapProcessor.shutdown();
      this.lastDeliveredZxid = commitProcessor.getLastDeliveredZxid();
      this.participantState.updateLastDeliveredZxid(this.lastDeliveredZxid);
    }
  }

  void onJoin(MessageTuple tuple, PreProcessor preProcessor,
              AckProcessor ackProcessor, CommitProcessor commitProcessor,
              ClusterConfiguration clusterConfig)
      throws IOException {
    // For JOIN message, we simply create a PeerHandler and add to quorum set
    // of main thread and pending set. Then we pass the JOIN message to
    // PreProcessor and AckProcessor. PreProcessor will convert the JOIN
    // request to COP proposal.
    String source = tuple.getServerId();
    PeerHandler ph = new PeerHandler(source, transport, config.getTimeout()/3);
    // For joiner, its history must be empty.
    ph.setLastZxid(Zxid.ZXID_NOT_EXIST);
    // We'll synchronize the joiner up to last proposed zxid of leader.
    ph.setLastSyncedZxid(tuple.getZxid());
    clusterConfig.addPeer(source);
    quorumSet.put(source, ph);
    pendingPeers.put(source, ph);
    preProcessor.processRequest(tuple);
    ackProcessor.processRequest(tuple);
    commitProcessor.processRequest(tuple);
  }

  void onCommit(MessageTuple tuple, CommitProcessor commitProcessor) {
    Message msg = tuple.getMessage();
    this.lastCommittedZxid =
      MessageBuilder.fromProtoZxid(msg.getCommit().getZxid());
    // If there's a pending COP we need to find out whether it can be committed.
    if (pendingCopZxid != null &&
        lastCommittedZxid.compareTo(this.pendingCopZxid) >= 0) {
      LOG.debug("COP of {} has been committed.", pendingCopZxid);
      // Resets it to null to allow for next reconfiguration.
      pendingCopZxid = null;
      if (stateChangeCallback != null) {
        stateChangeCallback.commitCop();
      }
    }
    if (!pendingPeers.isEmpty()) {
      // If there're any pending followers who are waiting for synchronization
      // complete we need to checkout whether we can send out the first COMMIT
      // message to them.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got message {} and there're pending peers.",
                  TextFormat.shortDebugString(msg));
      }
      Zxid zxidCommit = this.lastCommittedZxid;
      Iterator<Map.Entry<String, PeerHandler>> iter =
        pendingPeers.entrySet().iterator();
      while (iter.hasNext()) {
        PeerHandler ph = iter.next().getValue();
        Zxid ackZxid = ph.getLastAckedZxid();
        if (ackZxid != null && zxidCommit.compareTo(ackZxid) >= 0) {
          LOG.debug("COMMIT >= last acked zxid {} of pending peer. Send COMMIT",
                    ackZxid);
          Message commit = MessageBuilder.buildCommit(ackZxid);
          sendMessage(ph.getServerId(), commit);
          ph.startBroadcastingTask();
          iter.remove();
        }
      }
    }
    commitProcessor.processRequest(tuple);
  }

  void onDisconnected(MessageTuple tuple,
                      PreProcessor preProcessor,
                      AckProcessor ackProcessor,
                      CommitProcessor commitProcessor) {
    Message msg = tuple.getMessage();
    String peerId = msg.getDisconnected().getServerId();
    // Remove if it's in pendingPeers.
    pendingPeers.remove(peerId);
    PeerHandler ph = this.quorumSet.get(peerId);
    // Before calling shutdown, we need to disable PeerHandler first to prevent
    // sending obsolete messages in AckProcessor and preProcessor. Because once
    // we call shutdown(), the new connection from the peer is allowed, and
    // AckProcessor and PreProcessor should not send obsolete messages in new
    // connection.
    ph.disableSending();
    // Stop PeerHandler thread and clear tranport.
    ph.shutdown();
    quorumSet.remove(peerId);
    preProcessor.processRequest(tuple);
    ackProcessor.processRequest(tuple);
    commitProcessor.processRequest(tuple);
  }

  Zxid onAck(MessageTuple tuple,
             AckProcessor ackProcessor) throws IOException {
    String source = tuple.getServerId();
    Message msg = tuple.getMessage();
    ZabMessage.Ack ack = msg.getAck();
    Zxid ackZxid = MessageBuilder.fromProtoZxid(ack.getZxid());
    PeerHandler peer = pendingPeers.get(source);
    if (peer != null) {
      // This is the ACK sent from the peer at the end of the synchronization.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got first {} from pending peer {}.",
                  TextFormat.shortDebugString(msg),
                  source);
      }
      peer.setLastAckedZxid(ackZxid);
      if (lastCommittedZxid.compareTo(ackZxid) >= 0) {
        // If the zxid of ACK is already committed, send the first COMMIT to
        // follower and start broadcasting task. Otherwise we need to wait
        // until the zxid of ACK is committed.
        LOG.debug("The ACK of pending peer is committed already, send COMMIT.");
        Message commit = MessageBuilder.buildCommit(ackZxid);
        sendMessage(peer.getServerId(), commit);
        peer.startBroadcastingTask();
        pendingPeers.remove(source);
      }
    }
    if  (source.equals(this.serverId)) {
      // If the ACK comes from the leader, we need to update the last
      // acknowledged zxid of the leader.
      this.lastAckedZxid = ackZxid;
      if (!pendingPeers.isEmpty()) {
        // If there's any pending peer, we need also check if someone are
        // waiting for given proposal to be synchronized to log before the
        // synchronization can be started.
        for (PeerHandler ph : pendingPeers.values()) {
          if (!ph.isSyncStarted() &&
              ackZxid.compareTo(ph.getLastSyncedZxid()) >= 0) {
            ph.setSyncTask(new SyncPeerTask(ph.getServerId(),
                                            ph.getLastZxid(),
                                            ph.getLastSyncedZxid(),
                                            persistence.getLastSeenConfig()),
                           establishedEpoch);
            ph.startSynchronizingTask();
          }
        }
      }
    }
    ackProcessor.processRequest(tuple);
    return ackZxid;
  }

  void onAckEpoch(MessageTuple tuple,
                  PreProcessor preProcessor,
                  AckProcessor ackProcessor,
                  CommitProcessor commitProcessor) throws IOException {
    String source = tuple.getServerId();
    Message msg = tuple.getMessage();
    // This is the last proposed zxid from leader, we'll synchronize the
    // follower up to this zxid.
    Zxid zxid = tuple.getZxid();
    ZabMessage.AckEpoch ackEpoch = msg.getAckEpoch();
    Zxid lastPeerZxid = MessageBuilder
                        .fromProtoZxid(ackEpoch.getLastZxid());
    PeerHandler ph = new PeerHandler(source, transport, config.getTimeout()/3);
    ph.setLastZxid(lastPeerZxid);
    ph.setLastSyncedZxid(zxid);
    // Add to the quorum set of main thread.
    quorumSet.put(source, ph);
    // Add to the pending set also.
    this.pendingPeers.put(source, ph);
    // Add new recovered follower to PreProcessor.
    preProcessor.processRequest(tuple);
    // Add new recovered follower to AckProcessor.
    ackProcessor.processRequest(tuple);
    // Also ask CommitProcessor to notify the clients of membership changes.
    commitProcessor.processRequest(tuple);
    if (lastAckedZxid.compareTo(zxid) >= 0) {
      // If current last proposed zxid is already in log, starts synchronization
      // immediately.
      ph.setSyncTask(new SyncPeerTask(ph.getServerId(),
                                      ph.getLastZxid(),
                                      ph.getLastSyncedZxid(),
                                      persistence.getLastSeenConfig()),
                     this.establishedEpoch);
      ph.startSynchronizingTask();
    }
  }

  void onRemove(MessageTuple tuple, PreProcessor preProcessor,
                AckProcessor ackProcessor,
                ClusterConfiguration clusterConfig) throws IOException {
    // NOTE : For REMOVE message, we shouldn't remove server from quorumSet
    // here, the leaving server will close the transport once the COP gets
    // committed and then we'll remove it like normal DISCONNECTED server.
    // this.quorumSet.remove(server);
    // But we still need to remove the server from PreProcessor since logically
    // all the proposals after COP will not be the responsibilities of removed
    // server.
    Message msg = tuple.getMessage();
    clusterConfig.removePeer(msg.getRemove().getServerId());
    preProcessor.processRequest(tuple);
    ackProcessor.processRequest(tuple);
  }

  void onFlushReq(MessageTuple tuple) {
    Message msg = tuple.getMessage();
    String source = tuple.getServerId();
    PeerHandler ph = quorumSet.get(source);
    Zxid zxid = ph.getLastProposedZxid();
    ZabMessage.FlushRequest req = msg.getFlushRequest();
    msg = MessageBuilder.buildFlush(zxid,
                                    req.getBody().asReadOnlyByteBuffer());
    sendMessage(source, msg);
  }

  void checkFollowerLiveness() {
    long currentTime = System.nanoTime();
    long timeoutNs = this.config.getTimeout() * (long)1000000;
    for (PeerHandler ph : this.quorumSet.values()) {
      if (ph.getServerId().equals(this.serverId)) {
        continue;
      }
      if (currentTime - ph.getLastHeartbeatTime() >= timeoutNs) {
        // Removes the peer who is likely to be dead.
        String peerId = ph.getServerId();
        LOG.warn("{} is likely to be dead, enqueue a DISCONNECTED message.",
                 peerId);
        // Enqueue a DISCONNECTED message.
        Message disconnected = MessageBuilder.buildDisconnected(peerId);
        this.messageQueue.add(new MessageTuple(this.serverId,
                                               disconnected));
      }
    }
  }

  /**
   * Gets next proposed zxid for leader in broadcasting phase.
   *
   * @return the zxid of next proposed transaction.
   */
  private Zxid getNextProposedZxid() {
    if (lastProposedZxid.getEpoch() != establishedEpoch) {
      lastProposedZxid = new Zxid(establishedEpoch, -1);
    }
    lastProposedZxid = new Zxid(establishedEpoch,
                                lastProposedZxid.getXid() + 1);
    return lastProposedZxid;
  }
}
