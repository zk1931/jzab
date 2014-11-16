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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.ArrayList;
import java.util.List;
import com.github.zk1931.jzab.proto.ZabMessage;
import com.github.zk1931.jzab.proto.ZabMessage.Message;
import com.github.zk1931.jzab.proto.ZabMessage.Message.MessageType;
import com.github.zk1931.jzab.transport.Transport;
import com.github.zk1931.jzab.Zab.FailureCaseCallback;
import com.github.zk1931.jzab.Zab.StateChangeCallback;
import com.github.zk1931.jzab.ZabException.NotBroadcastingPhase;
import com.github.zk1931.jzab.ZabException.TooManyPendingRequests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.github.zk1931.jzab.proto.ZabMessage.Proposal.ProposalType;

/**
 * Participant is the base class for different roles of Jzab(Leader/Follower).
 */
public abstract class Participant {
  /**
   * Transport is used for communication between different Zab instances.
   */
  protected final Transport transport;

  /**
   * Persistent state of Zab.
   */
  protected final PersistentState persistence;

  /**
   * State machine callback.
   */
  protected final StateMachine stateMachine;

  /**
   * Server Id of the participant.
   */
  protected final String serverId;

  /**
   * Shared message queue. Passed from Zab and all the messages are
   * enqueued from Zab.
   */
  protected final BlockingQueue<MessageTuple> messageQueue;

  /**
   * The last delivered zxid. Passed from Zab to avoid deliver duplicated
   * transactions.
   */
  protected Zxid lastDeliveredZxid;

  /**
   * The elected leader.
   */
  protected String electedLeader;

  /**
   * The configuration for Zab. It's passed from Zab.
   */
  protected final ZabConfig config;

  /**
   * Callback which will be called in different points of code path to simulate
   * different kinds of failure cases.
   */
  protected FailureCaseCallback failCallback = null;

  /**
   * Callback interface for testing purpose.
   */
  protected StateChangeCallback stateChangeCallback = null;

  /**
   * The State for Zab. It will be passed accross different instances of
   * Leader/Follower class.
   */
  protected final ParticipantState participantState;

  /**
   * The number of pending requests (enqueued by calling send or flush). We
   * keep track of the number of pending requests because once the number of
   * pending requests exceeds certain threshold, we'll raise an exception to
   * user.
   */
  protected final AtomicInteger pendingReqs = new AtomicInteger(0);

  /**
   *  The maximum batch size for SyncProposalProcessor.
   */
  protected final int maxBatchSize;

  /**
   * The object used for election, the participant might receive election
   * message from peers when it's in non-electing phase, it still needs to
   * reply its vote to them.
   */
  protected final Election election;

  /**
   * Current phase of Participant.
   */
  protected Phase currentPhase = Phase.DISCOVERING;

  /**
   * Whether there's is a snapshot request in progress.
   */
  protected boolean isSnapshotInProgress = false;

  private static final Logger LOG = LoggerFactory.getLogger(Participant.class);

  protected enum Phase {
    DISCOVERING,
    SYNCHRONIZING,
    BROADCASTING,
    FINALIZING
  }

  /**
   * This exception is only used to force leader/follower goes back to election
   * phase.
   */
  static class BackToElectionException extends RuntimeException {
  }

  /**
   * This exception is used to make the server exits.
   */
  static class LeftCluster extends RuntimeException {
    public LeftCluster(String desc) {
      super(desc);
    }
  }

  /**
   * Failed to join cluster.
   */
  static class JoinFailure extends RuntimeException {
    public JoinFailure(String desc) {
      super(desc);
    }
  }

  public Participant(ParticipantState participantState,
                     StateMachine stateMachine,
                     ZabConfig config) {
    this.participantState = participantState;
    this.transport = participantState.getTransport();
    this.persistence = participantState.getPersistence();
    this.lastDeliveredZxid = participantState.getLastDeliveredZxid();
    this.serverId = participantState.getServerId();
    this.messageQueue = participantState.getMessageQueue();
    this.stateMachine = stateMachine;
    this.stateChangeCallback = participantState.getStateChangeCallback();
    this.failCallback = participantState.getFailureCaseCallback();
    this.config = config;
    this.election = participantState.getElection();
    this.maxBatchSize = config.getMaxBatchSize();
  }

  void send(ByteBuffer request)
      throws NotBroadcastingPhase, TooManyPendingRequests {
    if (this.currentPhase != Phase.BROADCASTING) {
      throw new NotBroadcastingPhase("Not in Broadcasting phase!");
    }
    if (pendingReqs.get() > ZabConfig.MAX_PENDING_REQS) {
      // If the size of pending requests exceeds the certain threshold, raise
      // TooManyPendingRequests exception.
      throw new TooManyPendingRequests();
    }
    // Increments the pending requests by one.
    pendingReqs.incrementAndGet();
    Message msg = MessageBuilder.buildRequest(request);
    sendMessage(this.electedLeader, msg);
  }

  void remove(String peerId) throws NotBroadcastingPhase {
    if (this.currentPhase != Phase.BROADCASTING) {
      throw new NotBroadcastingPhase("Not in Broadcasting phase!");
    }
    Message msg = MessageBuilder.buildRemove(peerId);
    sendMessage(this.electedLeader, msg);
  }

  void flush(ByteBuffer request)
      throws NotBroadcastingPhase, TooManyPendingRequests {
    if (this.currentPhase != Phase.BROADCASTING) {
      throw new NotBroadcastingPhase("Not in Broadcasting phase!");
    }
    if (pendingReqs.get() > ZabConfig.MAX_PENDING_REQS) {
      // If the size of pending requests exceeds the certain threshold, raise
      // TooManyPendingRequests exception.
      throw new TooManyPendingRequests();
    }
    // Increments the pending requests by one.
    pendingReqs.incrementAndGet();
    Message msg = MessageBuilder.buildFlushRequest(request);
    sendMessage(this.electedLeader, msg);
  }

  synchronized void takeSnapshot() throws ZabException {
    if (this.currentPhase != Phase.BROADCASTING) {
      throw new NotBroadcastingPhase("You cannot take snapshots while"
          + " Jzab is in Recovering phase");
    }
    if (this.isSnapshotInProgress) {
      throw new TooManyPendingRequests("A pending snapshot is in progress");
    }
    this.isSnapshotInProgress = true;
    Message msg = MessageBuilder.buildSnapshot(this.lastDeliveredZxid);
    sendMessage(this.serverId, msg);
  }

  protected abstract void join(String peer) throws Exception;

  /**
   * Sends message to the specific destination.
   *
   * @param dest the destination of the message.
   * @param message the message to be sent.
   */
  protected void sendMessage(String dest, Message message) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Sends message {} to {}.",
                TextFormat.shortDebugString(message),
                dest);
    }
    this.transport.send(dest, message);
  }

  /**
   * Gets a message from the queue.
   *
   * @return a message tuple contains the message and its source.
   * @throws TimeoutException in case of timeout.
   * @throws InterruptedException it's interrupted.
   */
  protected MessageTuple getMessage()
      throws TimeoutException, InterruptedException {
    return getMessage(this.config.getTimeoutMs());
  }

  /**
   * Gets a message from the queue.
   *
   * @param timeoutMs how to wait before raising a TimeoutException.
   * @return a message tuple contains the message and its source.
   * @throws TimeoutException in case of timeout.
   * @throws InterruptedException it's interrupted.
   */
  protected abstract MessageTuple getMessage(int timeoutMs)
      throws TimeoutException, InterruptedException;

  /**
   * Gets the expected message. It will discard messages of unexpected type
   * and source and returns only if the expected message is received.
   *
   * @param type the expected message type.
   * @param source the expected source, null if it can from anyone.
   * @param timeoutMs how long to wait before raising a TimeoutException.
   * @return the message tuple contains the message and its source.
   * @throws TimeoutException in case of timeout.
   * @throws InterruptedException it's interrupted.
   */
  protected MessageTuple getExpectedMessage(MessageType type,
                                            String source,
                                            int timeoutMs)
      throws TimeoutException, InterruptedException {
    int startTime = (int) (System.nanoTime() / 1000000);
    // Waits until the expected message is received.
    while (true) {
      MessageTuple tuple = getMessage(timeoutMs);
      String from = tuple.getServerId();
      if (tuple.getMessage().getType() == type &&
          (source == null || source.equals(from))) {
        // Return message only if it's expected type and expected source.
        return tuple;
      } else {
        int curTime = (int) (System.nanoTime() / 1000000);
        if (curTime - startTime >= timeoutMs) {
          throw new TimeoutException("Timeout in getExpectedMessage.");
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Got an unexpected message from {}: {}",
                    tuple.getServerId(),
                    TextFormat.shortDebugString(tuple.getMessage()));
        }
      }
    }
  }

  protected MessageTuple getExpectedMessage(MessageType type, String source)
      throws TimeoutException, InterruptedException {
    return getExpectedMessage(type, source, this.config.getTimeoutMs());
  }

  /**
   * Waits getting synchronizated from peer.
   *
   * @param peer the id of the expected peer that synchronization message will
   * come from.
   * @throws InterruptedException in case of interruption.
   * @throws TimeoutException in case of timeout.
   * @throws IOException in case of IO failure.
   */
  protected void waitForSync(String peer)
      throws InterruptedException, TimeoutException, IOException {
    LOG.debug("Waiting sync from {}.", peer);
    Log log = this.persistence.getLog();
    Zxid lastZxid = persistence.getLatestZxid();
    // The last zxid of peer.
    Zxid lastZxidPeer = null;
    Message msg = null;
    String source = null;
    // Expects getting message of DIFF or TRUNCATE or SNAPSHOT from peer or
    // PULL_TXN_REQ from leader.
    while (true) {
      MessageTuple tuple = getMessage(getSyncTimeoutMs());
      source = tuple.getServerId();
      msg = tuple.getMessage();
      if ((msg.getType() != MessageType.DIFF &&
           msg.getType() != MessageType.TRUNCATE &&
           msg.getType() != MessageType.SNAPSHOT &&
           msg.getType() != MessageType.PULL_TXN_REQ) ||
          !source.equals(peer)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Got unexpected message {} from {}.",
                    TextFormat.shortDebugString(msg), source);
        }
        continue;
      } else {
        break;
      }
    }
    if (msg.getType() == MessageType.PULL_TXN_REQ) {
      // PULL_TXN_REQ message. This message is only received at FOLLOWER side.
      LOG.debug("Got pull transaction request from {}",
                source);
      ZabMessage.Zxid z = msg.getPullTxnReq().getLastZxid();
      lastZxidPeer = MessageBuilder.fromProtoZxid(z);
      // Synchronize its history to leader.
      SyncPeerTask syncTask =
        new SyncPeerTask(source, lastZxidPeer, lastZxid,
                         this.persistence.getLastSeenConfig());
      syncTask.run();
      // After synchronization, leader should have same history as this
      // server, so next message should be an empty DIFF.
      MessageTuple tuple =
        getExpectedMessage(MessageType.DIFF, peer, getSyncTimeoutMs());
      msg = tuple.getMessage();
      ZabMessage.Diff diff = msg.getDiff();
      lastZxidPeer = MessageBuilder.fromProtoZxid(diff.getLastZxid());
      // Check if they match.
      if (lastZxidPeer.compareTo(lastZxid) != 0) {
        LOG.error("The history of leader and follower are not same.");
        throw new RuntimeException("Expecting leader and follower have same"
                                    + "history.");
      }
      waitForSyncEnd(peer);
      return;
    }
    if (msg.getType() == MessageType.DIFF) {
      // DIFF message.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got message {}",
                  TextFormat.shortDebugString(msg));
      }
      ZabMessage.Diff diff = msg.getDiff();
      // Remember last zxid of the peer.
      lastZxidPeer = MessageBuilder.fromProtoZxid(diff.getLastZxid());
      if(lastZxid.compareTo(lastZxidPeer) == 0) {
        // Means the two nodes have exact same history.
        waitForSyncEnd(peer);
        return;
      }
    } else if (msg.getType() == MessageType.TRUNCATE) {
      // TRUNCATE message.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got message {}",
                  TextFormat.shortDebugString(msg));
      }
      ZabMessage.Truncate trunc = msg.getTruncate();
      Zxid lastPrefixZxid =
        MessageBuilder.fromProtoZxid(trunc.getLastPrefixZxid());
      lastZxidPeer = MessageBuilder.fromProtoZxid(trunc.getLastZxid());
      if (lastZxidPeer.equals(Zxid.ZXID_NOT_EXIST)) {
        // When the truncate zxid is <0, -1>, we treat this as a state
        // transfer even it might not be.
        persistence.beginStateTransfer();
      } else {
        log.truncate(lastPrefixZxid);
      }
      if (lastZxidPeer.compareTo(lastPrefixZxid) == 0) {
        waitForSyncEnd(peer);
        return;
      }
    } else {
      // SNAPSHOT message.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got message {}",
                 TextFormat.shortDebugString(msg));
      }
      ZabMessage.Snapshot snap = msg.getSnapshot();
      lastZxidPeer = MessageBuilder.fromProtoZxid(snap.getLastZxid());
      Zxid snapZxid = MessageBuilder.fromProtoZxid(snap.getSnapZxid());
      // Waiting for snapshot file to be received.
      msg = getExpectedMessage(MessageType.FILE_RECEIVED, peer).getMessage();
      // Turns the temp file to snapshot file.
      File file = new File(msg.getFileReceived().getFullPath());
      // If the message is SNAPSHOT, it's state transferring.
      persistence.beginStateTransfer();
      persistence.setSnapshotFile(file, snapZxid);
      // Truncates the whole log.
      log.truncate(Zxid.ZXID_NOT_EXIST);
      // Checks if there's any proposals after snapshot.
      if (lastZxidPeer.compareTo(snapZxid) == 0) {
        // If no, done with synchronization.
        waitForSyncEnd(peer);
        return;
      }
    }
    log = persistence.getLog();
    // Get subsequent proposals.
    while (true) {
      MessageTuple tuple = getExpectedMessage(MessageType.PROPOSAL, peer,
                                              getSyncTimeoutMs());
      msg = tuple.getMessage();
      source = tuple.getServerId();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got message {} from {}", TextFormat.shortDebugString(msg),
                  source);
      }
      ZabMessage.Proposal prop = msg.getProposal();
      Zxid zxid = MessageBuilder.fromProtoZxid(prop.getZxid());
      // Accept the proposal.
      log.append(MessageBuilder.fromProposal(prop));
      // Check if this is the last proposal.
      if (zxid.compareTo(lastZxidPeer) == 0) {
        waitForSyncEnd(peer);
        return;
      }
    }
  }

  /**
   * Waits for the end of the synchronization and updates last seen config
   * file.
   *
   * @param peerId the id of the peer.
   * @throws TimeoutException in case of timeout.
   * @throws IOException in case of IOException.
   * @throws InterruptedException if it's interrupted.
   */
  void waitForSyncEnd(String peerId)
      throws TimeoutException, IOException, InterruptedException {
    MessageTuple tuple = getExpectedMessage(MessageType.SYNC_END, peerId,
                                            getSyncTimeoutMs());
    ClusterConfiguration cnf
      = ClusterConfiguration.fromProto(tuple.getMessage().getConfig(),
                                       this.serverId);
    LOG.debug("Got SYNC_END {} from {}", cnf, peerId);
    this.persistence.setLastSeenConfig(cnf);
    if (persistence.isInStateTransfer()) {
      persistence.endStateTransfer();
    }
  }

  /**
   * Returns all the transactions appear in log(It's for testing purpose only).
   *
   * @return a list of transactions.
   */
  List<Transaction> getAllTxns() throws IOException {
    Log log = this.persistence.getLog();
    List<Transaction> txns = new ArrayList<Transaction>();
    try(Log.LogIterator iter = log.getIterator(new Zxid(0, 0))) {
      while(iter.hasNext()) {
        txns.add(iter.next());
      }
      return txns;
    }
  }

  /**
   * Restores the state of user's application from snapshot file.
   */
  void restoreFromSnapshot() throws IOException {
    Zxid snapshotZxid = persistence.getSnapshotZxid();
    LOG.debug("The last applied zxid in snapshot is {}", snapshotZxid);
    if (snapshotZxid != Zxid.ZXID_NOT_EXIST &&
        lastDeliveredZxid == Zxid.ZXID_NOT_EXIST) {
      // Restores from snapshot only if the lastDeliveredZxid == ZXID_NOT_EXIST,
      // which means it's the application's first time recovery.
      File snapshot = persistence.getSnapshotFile();
      try (FileInputStream fin = new FileInputStream(snapshot)) {
        LOG.debug("Restoring application's state from snapshot {}", snapshot);
        stateMachine.restore(fin);
        lastDeliveredZxid = snapshotZxid;
      }
    }
  }

  /**
   * Delivers all the transactions in the log after last delivered zxid.
   *
   * @throws IOException in case of IO failures.
   */
  void deliverUndeliveredTxns() throws IOException {
    LOG.debug("Delivering all the undelivered txns after {}",
              lastDeliveredZxid);
    Zxid startZxid =
      new Zxid(lastDeliveredZxid.getEpoch(), lastDeliveredZxid.getXid() + 1);
    Log log = persistence.getLog();
    try (Log.LogIterator iter = log.getIterator(startZxid)) {
      while (iter.hasNext()) {
        Transaction txn = iter.next();
        if (txn.getType() == ProposalType.USER_REQUEST_VALUE) {
          // Only delivers REQUEST proposal, ignores COP.
          stateMachine.deliver(txn.getZxid(), txn.getBody(), null);
        }
        lastDeliveredZxid = txn.getZxid();
      }
    }
  }

  /**
   * Change the phase of current participant.
   *
   * @param phase the phase change(DISCOVERING/SYNCHRONIZING/BROADCASTING).
   * @throws IOException in case of IO failure.
   * @throws InterruptedException in case of interruption.
   */
  protected abstract void changePhase(Phase phase)
      throws IOException, InterruptedException, ExecutionException;

  /**
   * Handled the DELIVERED message comes from CommitProcessor.
   */
  protected void onDelivered(Message msg, SnapshotProcessor snapProcessor) {
    // Updates last delivered zxid.
    this.lastDeliveredZxid =
      MessageBuilder.fromProtoZxid(msg.getDelivered().getZxid());
  }

  protected void onFlush(MessageTuple tuple, CommitProcessor commitProcessor) {
    commitProcessor.processRequest(tuple);
  }

  /**
   * Clears all the messages in the message queue, clears the peer in transport
   * if it's the DISCONNECTED message. This function should be called only
   * right before going back to recovery.
   */ protected void clearMessageQueue() {
    MessageTuple tuple = null;
    while ((tuple = messageQueue.poll()) != null) {
      Message msg = tuple.getMessage();
      if (msg.getType() == MessageType.DISCONNECTED) {
        this.transport.clear(msg.getDisconnected().getServerId());
      } else if (msg.getType() == MessageType.SHUT_DOWN) {
        throw new LeftCluster("Shutdown Zab.");
      }
    }
  }

  /**
   * Gets current synchronizing timeout in milliseconds.
   *
   * @return synchronizing timeuout in milliseconds.
   */
  protected int getSyncTimeoutMs() {
    return participantState.getSyncTimeoutMs();
  }

  /**
   * Doubles the synchronizing timeout.
   *
   * @return the old synchronizing timeout in milliseconds.
   */
  protected int incSyncTimeout() {
    int syncTimeoutMs = getSyncTimeoutMs();
    participantState.setSyncTimeoutMs(syncTimeoutMs * 2);
    LOG.debug("Increase the sync timeout to {}", getSyncTimeoutMs());
    return syncTimeoutMs;
  }

  /**
   * Sets the synchronizing timeout.
   *
   * @param timeout the new synchronizing timeout in milliseconds.
   */
  protected void setSyncTimeoutMs(int timeout) {
    participantState.setSyncTimeoutMs(timeout);
  }

  /**
   * Adjusts the synchronizing timeout based on parameter timeoutMs. The new
   * synchronizing timeout will be smallest timeout which is power of 2 and
   * larger than timeoutMs.
   *
   * @param timeoutMs the last synchronizing timeout in milliseconds.
   */
  protected void adjustSyncTimeout(int timeoutMs) {
    int timeoutSec = (timeoutMs + 999) / 1000;
    // Finds out the smallest timeout which is powers of 2 and larger than
    // timeoutMs.
    for (int i = 0; i < 32; ++i) {
      if ((1 << i) >= timeoutSec) {
        LOG.debug("Adjust timeout to {} sec", 1 << i);
        setSyncTimeoutMs((1 << i) * 1000);
        return;
      }
    }
    throw new RuntimeException("Timeout is too large!");
  }

  /**
   * Synchronizes server's history to peer based on the last zxid of peer.
   * This function is called when the follower syncs its history to leader as
   * initial history or the leader syncs its initial history to followers in
   * synchronization phase. Based on the last zxid of peer, the synchronization
   * can be performed by TRUNCATE, DIFF or SNAPSHOT. The assumption is that
   * the server has all the committed transactions in its transaction log or
   * snapshot.
   *
   * Some invariants:
   *
   * 1. PersistentState.getLatestZxid() returns the latest zxid which is
   * guaranteed on disk, it can be either from log or snapshot.
   *
   * 2. The latest zxid of the log is always equal or greater than the lastest
   * zxid of the snapshot file if the log is non-empty. The only exception is
   * that the log is empty.
   *
   * 3. During the synchronization, we'll only truncate the transaction which
   * hasn't been committed.
   *
   * 4. The zxid of the snapshot file had always been committed in log, so if
   * the zxid of peer is from its snapshot we'll never truncate this.
   *
   * 5. The zxid returned from PersistentState.getLatestZxid() of leader is
   * guaranteed to be equal or greater than all the committed transactions.
   */
  protected class SyncPeerTask {
    private final String peerId;
    private final Zxid peerLatestZxid;
    private final Zxid lastSyncZxid;
    private final ClusterConfiguration clusterConfig;
    private boolean stop = false;

   /**
    * Constructs the SyncPeerTask object.
    *
    * @param peerId the id of the peer
    * @param peerLatestZxid the last zxid of the peer
    * @param lastSyncZxid leader will synchronize the follower up to this zxid
    * @param config the configuration send to peer at the end of synchronization
    */
    public SyncPeerTask(String peerId, Zxid peerLatestZxid, Zxid lastSyncZxid,
                        ClusterConfiguration config) {
      this.peerId = peerId;
      this.peerLatestZxid = peerLatestZxid;
      this.lastSyncZxid = lastSyncZxid;
      this.clusterConfig = config;
    }

    public Zxid getLastSyncZxid() {
      return this.lastSyncZxid;
    }

    public void stop() {
      this.stop = true;
    }

    void stateTransfer(File snapFile, Zxid snapZxid, Log log)
        throws IOException {
      // stateTransfer will transfer the whole state (snapshot + log) to peer.
      Zxid syncPoint = Zxid.ZXID_NOT_EXIST;
      if (snapFile != null) {
        // Synchronizing peer with snapshot file first.
        Message snapshot = MessageBuilder.buildSnapshot(lastSyncZxid, snapZxid);
        // Sends snapshot message.
        transport.send(peerId, snapshot);
        // Sends actuall snapshot file.
        transport.send(peerId, snapFile);
        // Start synchronizing from the zxid right after snapshot.
        syncPoint = new Zxid(snapZxid.getEpoch(), snapZxid.getXid() + 1);
      } else {
        // If we don't have snapshot truncate all the transactions of peers.
        Message truncate =
          MessageBuilder.buildTruncate(Zxid.ZXID_NOT_EXIST, lastSyncZxid);
        sendMessage(peerId, truncate);
      }
      try (Log.LogIterator iter = log.getIterator(syncPoint)) {
        while (iter.hasNext()) {
          if (stop) {
            return;
          }
          Transaction txn = iter.next();
          if (txn.getZxid().compareTo(this.lastSyncZxid) > 0) {
            break;
          }
          Message prop = MessageBuilder.buildProposal(txn);
          sendMessage(peerId, prop);
        }
      }
    }

    void syncFromLog(Log log) throws IOException {
      // Synchronizes peer from log.
      Log.DivergingTuple dp = log.firstDivergingPoint(peerLatestZxid);
      Zxid zxid = dp.getDivergingZxid();
      if (zxid.compareTo(peerLatestZxid) == 0) {
        // Means there's no diverging point, send DIFF.
        Message diff = MessageBuilder.buildDiff(lastSyncZxid);
        sendMessage(peerId, diff);
      } else {
        // The zxid is the first diverging point, truncate after this.
        Message trunc = MessageBuilder.buildTruncate(zxid, lastSyncZxid);
        sendMessage(peerId, trunc);
      }
      try (Log.LogIterator iter = dp.getIterator()) {
        while (iter.hasNext()) {
          if (stop) {
            return;
          }
          Transaction txn = iter.next();
          Message prop = MessageBuilder.buildProposal(txn);
          sendMessage(peerId, prop);
        }
      }
    }

    public void run() throws IOException {
      LOG.debug("Starts synchronizing {}", peerId);
      Log log = persistence.getLog();
      Zxid snapZxid = persistence.getSnapshotZxid();
      File snapFile = persistence.getSnapshotFile();

      LOG.debug("Last peer zxid is {}, last sync zxid is {}, snapshot is {}",
                peerLatestZxid, lastSyncZxid, snapZxid);

      if (peerLatestZxid.equals(lastSyncZxid)) {
        // If the server has the same zxid with the peer's, just send an empty
        // DIFF.
        LOG.debug("Peer {} has same latest zxid zxid {}, send empty DIFF.",
                  peerId, lastSyncZxid);
        Message diff = MessageBuilder.buildDiff(lastSyncZxid);
        sendMessage(peerId, diff);
      } else if (peerLatestZxid.compareTo(lastSyncZxid) > 0) {
        LOG.debug("Peer {} has larger latest zxid zxid {}.",
                  peerId, peerLatestZxid);
        // The peer's latest zxid on disk is larger than the server's, which
        // means the peerLatestZxid must come from its log file instead of
        // snapshot file.
        if (peerLatestZxid.getEpoch() == lastSyncZxid.getEpoch()) {
          // Means the last zxids the servers are same, which means the server's
          // log history is a prefix of the peer's, just send truncate.
          Message trunc =
            MessageBuilder.buildTruncate(lastSyncZxid, lastSyncZxid);
          sendMessage(peerId, trunc);
        } else {
          // Not sure if the server's history is the prefix of the peer's, do
          // whole state transfer for simplicity.
          LOG.debug("The peer's zxid has different epoch, start state "
              + "tranfering.");
          stateTransfer(snapFile, snapZxid, log);
        }
      } else {
        // The peer's lastest zxid < the server's.
        if (snapZxid != null && snapZxid.compareTo(peerLatestZxid) >= 0) {
          stateTransfer(snapFile, snapZxid, log);
        } else {
          syncFromLog(log);
        }
      }
      if (this.stop) {
        return;
      }
      // At the end of the synchronization, send COP.
      Message syncEnd = MessageBuilder.buildSyncEnd(this.clusterConfig);
      sendMessage(peerId, syncEnd);
    }
  }
}
