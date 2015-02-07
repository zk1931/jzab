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

import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import com.github.zk1931.jzab.proto.ZabMessage;
import com.github.zk1931.jzab.proto.ZabMessage.Message;
import com.github.zk1931.jzab.proto.ZabMessage.Message.MessageType;
import com.github.zk1931.jzab.proto.ZabMessage.Proposal.ProposalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Leader.
 */
class Leader extends Participant {

  private final Map<String, PeerHandler> quorumMap =
    new ConcurrentHashMap<String, PeerHandler>();

  // Stores all the new joined/recovered servers who are waiting for
  // synchronization ends.
  private final Map<String, PeerHandler> pendingPeers =
    new HashMap<String, PeerHandler>();

  /**
   * The established epoch for this leader.
   */
  private long establishedEpoch = -1;

  /**
   * The zxid for pending COP, or null if there's no pending COP.
   */
  private Zxid pendingCopZxid = null;

  /**
   * PreProcessor converts requests to idempotent transactions.
   */
  private PreProcessor preProcessor = null;

  /**
   * Determines which transaction can be committed.
   */
  private AckProcessor ackProcessor = null;

  /**
   * The leader needs to maintain the last proposed zxid of itself, the last
   * acknowledged zxid from itself, and the last committed zxid it received.
   * The last two records are not necessarily most updated since the events
   * happen in different threads and the events will be passed along the
   * message pipeline, the leader only has the chance of catching the events in
   * main thread. But we can guarantee that in main thread there's no zxid
   * after lastProposedZxid has been proposed, and the last acknowledged and
   * committed zxid are actually acknowledged/committed.
   */
  private Zxid lastProposedZxid;
  private Zxid lastAckedZxid;
  private Zxid lastCommittedZxid;

  private static final Logger LOG = LoggerFactory.getLogger(Leader.class);

  public Leader(ParticipantState participantState,
                StateMachine stateMachine,
                ZabConfig config) {
    super(participantState, stateMachine, config);
    this.electedLeader = participantState.getServerId();
    filter = new LeaderFilter(messageQueue, election);
    MDC.put("state", "leading");
  }

  @Override
  protected void changePhase(Phase phase)
      throws IOException, InterruptedException, ExecutionException {
    this.currentPhase = phase;
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
      if (failCallback != null) {
        failCallback.leaderBroadcasting();
      }
      if (stateChangeCallback != null) {
        stateChangeCallback.leaderBroadcasting(persistence.getAckEpoch(),
                                               getAllTxns(),
                                               persistence.getLastSeenConfig());
      }
    } else if (phase == Phase.FINALIZING) {
      MDC.put("phase", "finalizing");
      stateMachine.recovering(pendings);
      if (persistence.isInStateTransfer()) {
        // If the participant goes back to recovering phase in state
        // transferring mode, we need to explicitly undo the state transferring.
        persistence.undoStateTransfer();
      }
      // Shuts down all the handler of followers.
      for (PeerHandler ph : this.quorumMap.values()) {
        ph.shutdown();
        this.quorumMap.remove(ph.getServerId());
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
        new ClusterConfiguration(new Zxid(0, 0), peers, this.serverId);
      persistence.setLastSeenConfig(cnf);
      ByteBuffer cop = cnf.toByteBuffer();
      Transaction txn =
        new Transaction(cnf.getVersion(), ProposalType.COP_VALUE, cop);
      // Also we need to append the initial configuration to log.
      persistence.getLog().append(txn);
      persistence.setProposedEpoch(0);
      persistence.setAckEpoch(0);

      /* -- Broadcasting phase -- */
      // Initialize the vote for leader election.
      this.election.specifyLeader(this.serverId);
      changePhase(Phase.BROADCASTING);
      broadcasting();
    } catch (InterruptedException e) {
      LOG.debug("Participant is canceled by user.");
      throw e;
    } catch (TimeoutException e) {
      LOG.debug("Didn't hear message from peers for {} milliseconds. Going"
                + " back to leader election.",
                this.config.getTimeoutMs());
    } catch (BackToElectionException e) {
      LOG.debug("Got GO_BACK message from queue, going back to electing.");
    } catch (LeftCluster e) {
      LOG.debug("Exit running : {}", e.getMessage());
      throw e;
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      throw e;
    } finally {
      changePhase(Phase.FINALIZING);
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
      LOG.debug("Synchronizing...");
      changePhase(Phase.SYNCHRONIZING);
      if (!peerId.equals(this.serverId)) {
        // Pulls history from the follower.
        synchronizeFromFollower(peerId);
      }
      // Updates ACK EPOCH of leader.
      persistence.setAckEpoch(persistence.getProposedEpoch());
      // Starts synchronizing.
      long st = System.nanoTime();
      beginSynchronizing();
      waitNewLeaderAckFromQuorum();
      long syncTime = System.nanoTime() - st;
      // Adjusts the sync timeout based on this synchronization time.
      adjustSyncTimeout((int)(syncTime / 1000000));

      // After receiving ACKs from all peers in quorum, broadcasts COMMIT
      // message to all peers in quorum map.
      broadcastCommitMessage();
      // See if it can be restored from the snapshot file.
      restoreFromSnapshot();
      // Delivers all the txns in log before entering broadcasting phase.
      deliverUndeliveredTxns();

      /* -- Broadcasting phase -- */
      changePhase(Phase.BROADCASTING);
      for (PeerHandler ph : this.quorumMap.values()) {
        ph.startBroadcastingTask();
        ph.updateHeartbeatTime();
      }
      broadcasting();
    } catch (InterruptedException e) {
      LOG.debug("Participant is canceled by user.");
      throw e;
    } catch (TimeoutException e) {
      LOG.debug("Didn't hear message from peers for {} milliseconds. Going"
                + " back to leader election.",
                this.config.getTimeoutMs());
    } catch (BackToElectionException e) {
      LOG.debug("Got GO_BACK message from queue, going back to electing.");
    } catch (Zab.SimulatedException e) {
      LOG.debug("Got SimulatedException, go back to leader election.");
    } catch (LeftCluster e) {
      LOG.debug("Exit running : {}", e.getMessage());
      throw e;
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      throw e;
    } finally {
      if (this.currentPhase == Phase.SYNCHRONIZING) {
        incSyncTimeout();
        LOG.debug("Go back to recovery in synchronization phase, increase " +
            "sync timeout to {} milliseconds.", getSyncTimeoutMs());
      }
      changePhase(Phase.FINALIZING);
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
    long acknowledgedEpoch = persistence.getAckEpoch();
    // Waits PROPOED_EPOCH from a quorum of peers in current configuraion.
    while (this.quorumMap.size() < getQuorumSize() - 1) {
      MessageTuple tuple = filter.getExpectedMessage(MessageType.PROPOSED_EPOCH,
                                                     null,
                                                     config.getTimeoutMs());
      Message msg = tuple.getMessage();
      String source = tuple.getServerId();
      ZabMessage.ProposedEpoch epoch = msg.getProposedEpoch();
      ClusterConfiguration peerConfig =
        ClusterConfiguration.fromProto(epoch.getConfig(), source);
      long peerProposedEpoch = epoch.getProposedEpoch();
      long peerAckedEpoch = epoch.getCurrentEpoch();
      int syncTimeoutMs = epoch.getSyncTimeout();
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
      if (this.quorumMap.containsKey(source)) {
        throw new RuntimeException("Quorum set has already contained "
            + source + ", probably a bug?");
      }
      LOG.debug("Got PROPOSED_EPOCH from {}", source);
      PeerHandler ph = new PeerHandler(source, transport,
                                       config.getTimeoutMs()/3);
      ph.setLastProposedEpoch(peerProposedEpoch);
      ph.setSyncTimeoutMs(syncTimeoutMs);
      this.quorumMap.put(source, ph);
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
    long maxEpoch = persistence.getProposedEpoch();
    int maxSyncTimeoutMs = getSyncTimeoutMs();
    for (PeerHandler ph : this.quorumMap.values()) {
      if (ph.getLastProposedEpoch() > maxEpoch) {
        maxEpoch = ph.getLastProposedEpoch();
      }
      if (ph.getSyncTimeoutMs() > maxSyncTimeoutMs) {
        maxSyncTimeoutMs = ph.getSyncTimeoutMs();
      }
    }
    // The new epoch number should be larger than any follower's epoch.
    long newEpoch = maxEpoch + 1;
    // Updates leader's last proposed epoch.
    persistence.setProposedEpoch(newEpoch);
    // Updates leader's sync timeout to the largest timeout found in the quorum.
    setSyncTimeoutMs(maxSyncTimeoutMs);
    LOG.debug("Begins proposing new epoch {} with sync timeout {} ms",
              newEpoch, getSyncTimeoutMs());
    // Sends new epoch message to quorum.
    broadcast(this.quorumMap.keySet().iterator(),
              MessageBuilder.buildNewEpochMessage(newEpoch,
                                                  getSyncTimeoutMs()));
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
    while (ackCount < this.quorumMap.size()) {
      MessageTuple tuple = filter.getExpectedMessage(MessageType.ACK_EPOCH,
                                                     null,
                                                     config.getTimeoutMs());
      Message msg = tuple.getMessage();
      String source = tuple.getServerId();
      if (!this.quorumMap.containsKey(source)) {
        LOG.warn("The Epoch ACK comes from {} who is not in quorum set, "
                 + "possibly from previous epoch?",
                 source);
        continue;
      }
      ackCount++;
      ZabMessage.AckEpoch ackEpoch = msg.getAckEpoch();
      ZabMessage.Zxid zxid = ackEpoch.getLastZxid();
      // Updates follower's f.a and lastZxid.
      PeerHandler ph = this.quorumMap.get(source);
      ph.setLastAckedEpoch(ackEpoch.getAcknowledgedEpoch());
      ph.setLastZxid(MessageBuilder.fromProtoZxid(zxid));
    }
    LOG.debug("Received ACKs from the quorum set of size {}.",
              this.quorumMap.size() + 1);
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
    long ackEpoch = persistence.getAckEpoch();
    Zxid zxid = persistence.getLatestZxid();
    String peerId = this.serverId;
    Iterator<Map.Entry<String, PeerHandler>> iter;
    iter = this.quorumMap.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, PeerHandler> entry = iter.next();
      long fEpoch = entry.getValue().getLastAckedEpoch();
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
    Zxid lastZxid = persistence.getLatestZxid();
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
    Zxid lastZxid = persistence.getLatestZxid();
    while (completeCount < this.quorumMap.size()) {
      // Here we should use sync_timeout.
      MessageTuple tuple = filter.getExpectedMessage(MessageType.ACK, null,
                                                     getSyncTimeoutMs());
      ZabMessage.Ack ack = tuple.getMessage().getAck();
      String source =tuple.getServerId();
      Zxid zxid = MessageBuilder.fromProtoZxid(ack.getZxid());
      if (!this.quorumMap.containsKey(source)) {
        LOG.warn("Quorum set doesn't contain {}, a bug?", source);
        continue;
      }
      if (zxid.compareTo(lastZxid) != 0) {
        LOG.error("The follower {} is not correctly synchronized.", source);
        throw new RuntimeException("The synchronized follower's last zxid"
            + "doesn't match last zxid of current leader.");
      }
      PeerHandler ph = this.quorumMap.get(source);
      ph.setLastAckedZxid(zxid);
      completeCount++;
    }
  }

  void broadcastCommitMessage() throws IOException {
    // Broadcasts commit message.
    Zxid zxid = persistence.getLatestZxid();
    Message commit = MessageBuilder.buildCommit(zxid);
    broadcast(this.quorumMap.keySet().iterator(), commit);
    for (PeerHandler ph : this.quorumMap.values()) {
      ph.setLastCommittedZxid(zxid);
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
    Zxid lastZxid = persistence.getLatestZxid();
    ClusterConfiguration clusterConfig = persistence.getLastSeenConfig();
    long proposedEpoch = persistence.getProposedEpoch();
    for (PeerHandler ph : this.quorumMap.values()) {
      ph.setSyncTask(new SyncPeerTask(ph.getServerId(), ph.getLastZxid(),
                                      lastZxid, clusterConfig),
                     proposedEpoch);
      ph.startSynchronizingTask();
    }
  }

  void broadcastingInit() throws IOException {
    Zxid lastZxid = persistence.getLatestZxid();
    this.establishedEpoch = persistence.getAckEpoch();
    // Gets the initial configuration at the beginning of broadcasting.
    clusterConfig = persistence.getLastSeenConfig();
    // Add leader itself to quorumMap.
    PeerHandler lh =
      new PeerHandler(serverId, transport, config.getTimeoutMs()/3);
    lh.setLastAckedZxid(lastZxid);
    lh.setLastCommittedZxid(lastZxid);
    lh.startBroadcastingTask();
    quorumMap.put(this.serverId, lh);
    this.preProcessor =
      new PreProcessor(stateMachine, quorumMap, clusterConfig);
    this.ackProcessor = new AckProcessor(quorumMap, clusterConfig);
    this.syncProcessor =
        new SyncProposalProcessor(persistence, transport, maxBatchSize);
    this.commitProcessor =
        new CommitProcessor(stateMachine, lastDeliveredZxid, serverId,
                            transport, quorumMap.keySet(),
                            clusterConfig, electedLeader, pendings);
    this.snapProcessor =
      new SnapshotProcessor(stateMachine, persistence, serverId, transport);
    // First time notifies the client active members and cluster configuration.
    stateMachine.leading(new HashSet<String>(quorumMap.keySet()),
                         new HashSet<String>(clusterConfig.getPeers()));
    this.lastCommittedZxid = lastZxid;
    this.lastProposedZxid = lastZxid;
    this.lastAckedZxid = lastZxid;
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
    // Initialization.
    broadcastingInit();
    try {
      while (this.quorumMap.size() >= clusterConfig.getQuorumSize()) {
        MessageTuple tuple = filter.getMessage(config.getTimeoutMs());
        Message msg = tuple.getMessage();
        String source = tuple.getServerId();
        // Checks if it's DISCONNECTED message.
        if (msg.getType() == MessageType.DISCONNECTED) {
          String peerId = msg.getDisconnected().getServerId();
          if (quorumMap.containsKey(peerId)) {
            onDisconnected(tuple);
          } else {
            this.transport.clear(peerId);
          }
          continue;
        }
        if (!quorumMap.containsKey(source)) {
          // Received a message sent from a peer who is outside the quorum.
          handleMessageOutsideQuorum(tuple);
        } else {
          // Received a message sent from the peer who is in quorum.
          handleMessageFromQuorum(tuple);
          PeerHandler ph = quorumMap.get(source);
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
      ackProcessor.shutdown();
      preProcessor.shutdown();
      commitProcessor.shutdown();
      syncProcessor.shutdown();
      snapProcessor.shutdown();
      this.lastDeliveredZxid = commitProcessor.getLastDeliveredZxid();
      this.participantState.updateLastDeliveredZxid(this.lastDeliveredZxid);
    }
  }

  void handleMessageOutsideQuorum(MessageTuple tuple) throws IOException {
    Message msg = tuple.getMessage();
    String source = tuple.getServerId();
    if (msg.getType() == MessageType.PROPOSED_EPOCH) {
      // Follower is in recovering while the leader is in broadcasting
      // phase, the leader just sends its current epoch as NEW_EPOCH message
      // to the follower directly. The synchronization to the recovered
      // follower will begin after receiving its ACK_EPOCH message.
      LOG.debug("Got PROPOSED_EPOCH from {}.", source);
      ClusterConfiguration cnf = persistence.getLastSeenConfig();
      if (!cnf.contains(source)) {
        // Only allows servers who are in the current config to join.
        LOG.warn("Got PROPOSED_EPOCH from {} who is not in config, "
            + "ignores it.", source);
        return;
      }
      int syncTimeoutMs = msg.getProposedEpoch().getSyncTimeout();
      if (syncTimeoutMs > getSyncTimeoutMs()) {
        // Updates leader's sync timeout if the peer's time out is larger.
        setSyncTimeoutMs(syncTimeoutMs);
      }
      Message newEpoch =
        MessageBuilder.buildNewEpochMessage(establishedEpoch,
                                            getSyncTimeoutMs());
      sendMessage(source, newEpoch);
    } else if (msg.getType() == MessageType.ACK_EPOCH) {
      LOG.debug("Got ACK_EPOCH from {}", source);
      // Got the ACK_EPOCH message, the leader will starts synchronizing to
      // the follower up to the last proposed zxid.
      tuple.setZxid(lastProposedZxid);
      onAckEpoch(tuple);
    } else if (msg.getType() == MessageType.QUERY_LEADER) {
      LOG.debug("Got QUERY_LEADER from {}", source);
      Message reply = MessageBuilder.buildQueryReply(this.serverId);
      sendMessage(source, reply);
    } else if (msg.getType() == MessageType.SYNC_HISTORY) {
      // The new joiner will issue SYNC_HISTORY message first to make its
      // history synchronized.
      onSyncHistory(tuple);
    } else if (msg.getType() == MessageType.ELECTION_INFO) {
      this.election.reply(tuple);
    } else {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Got unexpected message {} from {}.",
                  TextFormat.shortDebugString(msg),
                  source);
      }
    }
  }

  void handleMessageFromQuorum(MessageTuple tuple)
      throws ExecutionException, InterruptedException, IOException {
    Message msg = tuple.getMessage();
    String source = tuple.getServerId();
    if (msg.getType() != MessageType.HEARTBEAT && LOG.isDebugEnabled()) {
      LOG.debug("Got message {} from {}",
                TextFormat.shortDebugString(msg), source);
    }
    if (msg.getType() == MessageType.ACK) {
      onAck(tuple);
    } else if (msg.getType() == MessageType.REQUEST) {
      Zxid proposedZxid = getNextProposedZxid();
      // Updates last proposed zxid for this peer. The FLUSH
      // message needs this zxid to determine when it's safe
      // to deliver the FLUSH request back to user.
      quorumMap.get(source).setLastProposedZxid(proposedZxid);
      tuple.setZxid(proposedZxid);
      preProcessor.processRequest(tuple);
    } else if (msg.getType() == MessageType.FLUSH_REQ) {
      onFlushReq(tuple);
    } else if (msg.getType() == MessageType.FLUSH) {
      onFlush(tuple);
    } else if (msg.getType() == MessageType.HEARTBEAT) {
      LOG.trace("Got HEARTBEAT replies from {}", source);
    } else if (msg.getType() == MessageType.PROPOSAL) {
      onProposal(tuple);
    } else if (msg.getType() == MessageType.COMMIT) {
      onCommit(tuple);
    } else if (msg.getType() == MessageType.REMOVE) {
      if (pendingCopZxid != null) {
        LOG.warn("There's a pending reconfiguration still in progress.");
        return;
      }
      pendingCopZxid = getNextProposedZxid();
      tuple.setZxid(pendingCopZxid);
      onRemove(tuple);
    } else if (msg.getType() == MessageType.DELIVERED) {
      onDelivered(msg);
    } else if (msg.getType() == MessageType.JOIN) {
      LOG.debug("Got JOIN from {}", source);
      if (pendingCopZxid != null) {
        LOG.warn("There's a pending reconfiguration still in progress.");
        return;
      }
      pendingCopZxid = getNextProposedZxid();
      tuple.setZxid(pendingCopZxid);
      onJoin(tuple);
    } else if (msg.getType() == MessageType.SNAPSHOT) {
      snapProcessor.processRequest(tuple);
    } else if (msg.getType() == MessageType.SNAPSHOT_DONE) {
      commitProcessor.processRequest(tuple);
    } else {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Unexpected messgae : {} from {}",
                 TextFormat.shortDebugString(msg),
                 source);
      }
    }
  }

  void onJoin(MessageTuple tuple)
      throws IOException {
    // For JOIN message, we simply create a PeerHandler and add to quorum set
    // of main thread and pending set. Then we pass the JOIN message to
    // PreProcessor and AckProcessor. PreProcessor will convert the JOIN
    // request to COP proposal.
    Zxid lastZxid =
      MessageBuilder.fromProtoZxid(tuple.getMessage().getJoin().getLastZxid());
    String source = tuple.getServerId();
    PeerHandler ph =
      new PeerHandler(source, transport, config.getTimeoutMs()/3);
    // For joiner, its history must be empty.
    ph.setLastZxid(lastZxid);
    // We'll synchronize the joiner up to last proposed zxid of leader.
    ph.setLastSyncedZxid(tuple.getZxid());
    clusterConfig.addPeer(source);
    quorumMap.put(source, ph);
    pendingPeers.put(source, ph);
    preProcessor.processRequest(tuple);
    ackProcessor.processRequest(tuple);
    commitProcessor.processRequest(tuple);
  }

  void onCommit(MessageTuple tuple) {
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
          ph.setLastCommittedZxid(ackZxid);
          ph.startBroadcastingTask();
          iter.remove();
        }
      }
    }
    commitProcessor.processRequest(tuple);
  }

  void onDisconnected(MessageTuple tuple)
      throws InterruptedException, ExecutionException {
    Message msg = tuple.getMessage();
    String peerId = msg.getDisconnected().getServerId();
    // Remove if it's in pendingPeers.
    pendingPeers.remove(peerId);
    PeerHandler ph = this.quorumMap.get(peerId);
    // Before calling shutdown, we need to disable PeerHandler first to prevent
    // sending obsolete messages in AckProcessor and preProcessor. Because once
    // we call shutdown(), the new connection from the peer is allowed, and
    // AckProcessor and PreProcessor should not send obsolete messages in new
    // connection.
    ph.disableSending();
    // Stop PeerHandler thread and clear tranport.
    ph.shutdown();
    quorumMap.remove(peerId);
    preProcessor.processRequest(tuple);
    ackProcessor.processRequest(tuple);
    commitProcessor.processRequest(tuple);
  }

  Zxid onAck(MessageTuple tuple) throws IOException {
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
        peer.setLastCommittedZxid(ackZxid);
        // After sending out the first COMMIT message, we can start the
        // broadcasting task.
        peer.startBroadcastingTask();
        pendingPeers.remove(source);
      }
    }
    if (source.equals(this.serverId)) {
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
            // Gets the last configuration which is <= lastSynced zxid.
            ClusterConfiguration cnf =
              persistence.getLastConfigWithin(ph.getLastSyncedZxid());
            ph.setSyncTask(new SyncPeerTask(ph.getServerId(),
                                            ph.getLastZxid(),
                                            ph.getLastSyncedZxid(),
                                            cnf),
                           establishedEpoch);
            ph.startSynchronizingTask();
          }
        }
      }
    }
    ackProcessor.processRequest(tuple);
    return ackZxid;
  }

  void onAckEpoch(MessageTuple tuple) throws IOException {
    String source = tuple.getServerId();
    Message msg = tuple.getMessage();
    // This is the last proposed zxid from leader, we'll make it as the last
    // transaction of the synchronization.
    Zxid lastZxidOfSync = tuple.getZxid();
    ZabMessage.AckEpoch ackEpoch = msg.getAckEpoch();
    // Last zxid of the peer/follower.
    Zxid lastPeerZxid = MessageBuilder
                        .fromProtoZxid(ackEpoch.getLastZxid());
    PeerHandler ph =
      new PeerHandler(source, transport, config.getTimeoutMs()/3);
    ph.setLastZxid(lastPeerZxid);
    ph.setLastSyncedZxid(lastZxidOfSync);
    // Add to the quorum set of main thread.
    this.quorumMap.put(source, ph);
    // Add to the pending set also.
    this.pendingPeers.put(source, ph);
    // Add new recovered follower to PreProcessor.
    preProcessor.processRequest(tuple);
    // Add new recovered follower to AckProcessor.
    ackProcessor.processRequest(tuple);
    // Also ask CommitProcessor to notify the clients of membership changes.
    commitProcessor.processRequest(tuple);
    // Before starting the synchronization, we need to guarantee the last
    // proposed zxid appears in the leader's log. The way we guarantee this
    // is that the leader must have already acknowledged this proposal.
    // Otherwise we can only start synchronizing after receiving the
    // acknowledgement of the proposal from leader.
    if (lastAckedZxid.compareTo(lastZxidOfSync) >= 0) {
      // Great, the leader has already synchronized the last proposed
      // transaction to its log, we can start the synchronization right now.
      ClusterConfiguration cnf =
        persistence.getLastConfigWithin(ph.getLastSyncedZxid());
      ph.setSyncTask(new SyncPeerTask(ph.getServerId(),
                                      ph.getLastZxid(),
                                      ph.getLastSyncedZxid(),
                                      cnf),
                     this.establishedEpoch);
      ph.startSynchronizingTask();
    }
  }

  void onRemove(MessageTuple tuple) throws IOException {
    // NOTE : For REMOVE message, we shouldn't remove server from quorumMap
    // here, the leaving server will close the transport once the COP gets
    // committed and then we'll remove it like normal DISCONNECTED server.
    // this.quorumMap.remove(server);
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
    PeerHandler ph = quorumMap.get(source);
    Zxid zxid = ph.getLastProposedZxid();
    ZabMessage.FlushRequest req = msg.getFlushRequest();
    msg = MessageBuilder.buildFlush(zxid,
                                    req.getBody().asReadOnlyByteBuffer());
    sendMessage(source, msg);
  }

  void onSyncHistory(MessageTuple tuple) throws IOException {
    String source = tuple.getServerId();
    PeerHandler ph = new
      PeerHandler(source, transport, config.getTimeoutMs()/3);
    Zxid lastZxid = MessageBuilder.fromProtoZxid(tuple.getMessage()
                                                      .getSyncHistory()
                                                      .getLastZxid());
    // The new joiner will issue the SYNC_HISTORY message first. At this time
    // the joiner's log must be empty.
    ph.setLastZxid(lastZxid);
    // We'll synchronize the peer up to the last zxid that is guaranteed in the
    // leader's log.
    ph.setLastSyncedZxid(this.lastAckedZxid);
    ClusterConfiguration cnf =
      persistence.getLastConfigWithin(ph.getLastSyncedZxid());
    ph.setSyncTask(new SyncPeerTask(ph.getServerId(),
                                    ph.getLastZxid(),
                                    ph.getLastSyncedZxid(),
                                    cnf),
                   establishedEpoch);
    Message reply = MessageBuilder.buildSyncHistoryReply(getSyncTimeoutMs());
    // Sends the reply to tell new joiner the sync timeout.
    sendMessage(source, reply);
    ph.startSynchronizingTask();
    // Adds it to quorumMap.
    this.quorumMap.put(source, ph);
  }

  void checkFollowerLiveness() {
    long currentTime = System.nanoTime();
    long timeoutNs;
    for (PeerHandler ph : this.quorumMap.values()) {
      if (ph.getServerId().equals(this.serverId)) {
        continue;
      }
      if (ph.isDisconnected()) {
        // If has already been marked as disconnected, skips checking.
        continue;
      }
      // Uses different timeout depends if it's in synchronizing.
      if (!ph.isSynchronizing()) {
        timeoutNs = this.config.getTimeoutMs() * (long)1000000;
      } else {
        timeoutNs = getSyncTimeoutMs() * (long)1000000;
      }
      if (currentTime - ph.getLastHeartbeatTime() >= timeoutNs) {
        // Removes the peer who is likely to be dead.
        String peerId = ph.getServerId();
        LOG.debug("{} is likely to be dead, enqueue a DISCONNECTED message.",
                  peerId);
        // Enqueue a DISCONNECTED message.
        Message disconnected = MessageBuilder.buildDisconnected(peerId);
        this.messageQueue.add(new MessageTuple(this.serverId,
                                               disconnected));
        // Marks it as disconnected. Avoids duplicate check before receiving
        // DISCONNECTED message.
        ph.markDisconnected();
        if (ph.isSynchronizing()) {
          LOG.debug("Can't get heartbeat reply from {} in synchronizing phase.",
                    peerId);
          incSyncTimeout();
          LOG.debug("Adjusts sync timeout to {} ms", getSyncTimeoutMs());
        }
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

  /**
   * A filter class for leader acts as a successor of ElectionMessageFilter
   * class. It filters and handles DISCONNECTED message.
   */
  class LeaderFilter extends ElectionMessageFilter {
    LeaderFilter(BlockingQueue<MessageTuple> msgQueue, Election election) {
      super(msgQueue, election);
    }

    @Override
    protected MessageTuple getMessage(int timeoutMs)
        throws InterruptedException, TimeoutException {
      int startMs = (int)(System.nanoTime() / 1000000);
      while (true) {
        int nowMs = (int)(System.nanoTime() / 1000000);
        int remainMs = timeoutMs - (nowMs - startMs);
        if (remainMs < 0) {
          remainMs = 0;
        }
        MessageTuple tuple = super.getMessage(remainMs);
        if (tuple.getMessage().getType() == MessageType.DISCONNECTED) {
          // Got DISCONNECTED message enqueued by onDisconnected callback.
          Message msg = tuple.getMessage();
          String peerId = msg.getDisconnected().getServerId();
          if (quorumMap.containsKey(peerId)) {
            if (currentPhase != Phase.BROADCASTING) {
              // If you lost someone in your quorumMap before broadcasting
              // phase, you are for sure not have a quorum of followers, just go
              // back to leader election. The clearance of the transport happens
              // in the exception handlers of lead/join function.
              LOG.debug("Lost follower {} in the quorumMap in recovering.",
                        peerId);
              throw new BackToElectionException();
            } else {
              // Lost someone who is in the quorumMap in broadcasting phase,
              // return this message to caller and let it handles the
              // disconnection.
              LOG.debug("Lost follower {} in the quorumMap in broadcasting.",
                        peerId);
              return tuple;
            }
          } else {
            // Just lost someone you don't care, clear the transport so it can
            // join in in later time.
            LOG.debug("Lost follower {} outside quorumMap.", peerId);
            transport.clear(peerId);
          }
        } else {
          return tuple;
        }
      }
    }
  }
}
