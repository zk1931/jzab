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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.List;
import com.github.zk1931.jzab.proto.ZabMessage;
import com.github.zk1931.jzab.proto.ZabMessage.Message;
import com.github.zk1931.jzab.proto.ZabMessage.Message.MessageType;
import com.github.zk1931.jzab.QuorumZab.FailureCaseCallback;
import com.github.zk1931.jzab.QuorumZab.StateChangeCallback;
import com.github.zk1931.jzab.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.github.zk1931.jzab.proto.ZabMessage.Proposal.ProposalType;

/**
 * Participant.
 */
public abstract class Participant {
  /**
   * Transport. Passed from QuorumZab.
   */
  protected final Transport transport;

  /**
   * Persistent state of Zab. Passed from QuorumZab.
   */
  protected final PersistentState persistence;

  /**
   * State machine. Passed from QuorumZab.
   */
  protected final StateMachine stateMachine;

  /**
   * ID of the participant. Passed from QuorumZab.
   */
  protected final String serverId;

  /**
   * Shared message queue. Passed from QuorumZab and all the messages are
   * enqueued from QuorumZab.
   */
  protected final BlockingQueue<MessageTuple> messageQueue;

  /**
   * Whether the participant is in broadcasting phase or not.
   */
  protected volatile boolean isBroadcasting = false;

  /**
   * The last delivered zxid. Passed from QuorumZab to avoid deliver duplicated
   * transactions.
   */
  protected Zxid lastDeliveredZxid;

  /**
   * The elected leader.
   */
  protected String electedLeader;

  /**
   * Maximum batch size for SyncRequestProcessor.
   *
   * TODO We might want to expose this setting to the user.
   */
  protected static final int SYNC_MAX_BATCH_SIZE = 1000;

  /**
   * The configuration for Zab. It's passed from QuorumZab.
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
   * The maximum number of transactions can be appended to log before taking
   * snapshot.
   */
  protected final long snapshotThreshold;

  /**
   * Total number of bytes of delivered transactions since last snapshot.
   */
  protected long numDeliveredBytes = 0;

  /**
   * The semaphore is used to prevent the internal queues grow infinitely.
   */
  protected final Semaphore semPendingReqs =
    new Semaphore(ZabConfig.MAX_PENDING_REQS);

  private static final Logger LOG = LoggerFactory.getLogger(Participant.class);

  protected enum Phase {
    DISCOVERING,
    SYNCHRONIZING,
    BROADCASTING
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
    this.snapshotThreshold = config.getSnapshotThreshold();
    this.config = config;
  }

  protected abstract void join(String peer) throws Exception;

  protected void send(ByteBuffer request) {
    if (!this.isBroadcasting) {
      throw new RuntimeException("Only can send in broadcasting phase.");
    }
    Message msg = MessageBuilder.buildRequest(request);
    sendMessage(this.electedLeader, msg);
  }

  public void setStateChangeCallback(StateChangeCallback stCallback) {
    this.stateChangeCallback = stCallback;
  }

  public void setFailureCaseCallback(FailureCaseCallback flCallback) {
    this.failCallback = flCallback;
  }

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
  protected abstract MessageTuple getMessage()
      throws TimeoutException, InterruptedException;

  /**
   * Gets the expected message. It will discard messages of unexpected type
   * and source and returns only if the expected message is received.
   *
   * @param type the expected message type.
   * @param source the expected source, null if it can from anyone.
   * @return the message tuple contains the message and its source.
   * @throws TimeoutException in case of timeout.
   * @throws InterruptedException it's interrupted.
   */
  protected MessageTuple getExpectedMessage(MessageType type, String source)
      throws TimeoutException, InterruptedException {
    int startTime = (int) (System.nanoTime() / 1000000);
    // Waits until the expected message is received.
    while (true) {
      MessageTuple tuple = getMessage();
      String from = tuple.getServerId();
      if (tuple.getMessage().getType() == type &&
          (source == null || source.equals(from))) {
        // Return message only if it's expected type and expected source.
        return tuple;
      } else {
        int curTime = (int) (System.nanoTime() / 1000000);
        if (curTime - startTime >= this.config.getTimeout()) {
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

  /**
   * Waits for a synchronization message from peer.
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
    Zxid lastZxid = log.getLatestZxid();
    // The last zxid of peer.
    Zxid lastZxidPeer = null;
    Message msg = null;
    String source = null;
    // Expects getting message of DIFF or TRUNCATE or SNAPSHOT or PULL_TXN_REQ
    // from elected leader.
    while (true) {
      MessageTuple tuple = getMessage();
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
      MessageTuple tuple = getExpectedMessage(MessageType.DIFF, peer);
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
      log.truncate(lastPrefixZxid);
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
      persistence.setSnapshotFile(file, snapZxid);
      // Truncates the whole log.
      log.truncate(Zxid.ZXID_NOT_EXIST);
      // Checks if there's any proposals after snapshot.
      if (lastZxidPeer.compareTo(Zxid.ZXID_NOT_EXIST) == 0) {
        // If no, done with synchronization.
        waitForSyncEnd(peer);
        return;
      }
    }
    // Get subsequent proposals.
    while (true) {
      MessageTuple tuple = getExpectedMessage(MessageType.PROPOSAL, peer);
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
    MessageTuple tuple = getExpectedMessage(MessageType.SYNC_END, peerId);
    ClusterConfiguration cnf
      = ClusterConfiguration.fromProto(tuple.getMessage().getConfig(),
                                       this.serverId);
    LOG.debug("Got SYNC_END {} from {}", cnf, peerId);
    this.persistence.setLastSeenConfig(cnf);
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
   */
  protected abstract void changePhase(Phase phase) throws IOException;

  /**
   * Handled the DELIVERED message comes from CommitProcessor.
   */
  protected void onDelivered(Message msg, SnapshotProcessor snapProcessor) {
    // Updates last delivered zxid.
    this.lastDeliveredZxid =
      MessageBuilder.fromProtoZxid(msg.getDelivered().getZxid());
    // Gets number of bytes delivered for last commit message.
    long nBytes = msg.getDelivered().getNumBytes();
    this.numDeliveredBytes += nBytes;
    if (snapshotThreshold > 0 &&
      numDeliveredBytes  >= snapshotThreshold) {
      // Reaches the threshold, going to take snashot.
      LOG.debug("Delivered {} bytes to application since last " +
          "snapshot, going to take snapshot.", numDeliveredBytes);
      Message snap = MessageBuilder.buildSnapshot(lastDeliveredZxid);
      snapProcessor.processRequest(new MessageTuple(null, snap));
      // Resets it to zero.
      numDeliveredBytes = 0;
    }
  }

  protected void onFlush(MessageTuple tuple, CommitProcessor commitProcessor) {
    commitProcessor.processRequest(tuple);
  }

  /**
   * Synchronizes server's history to peer based on the last zxid of peer.
   * This function is called when the follower syncs its history to leader as
   * initial history or the leader syncs its initial history to followers in
   * synchronization phase. Based on the last zxid of peer, the synchronization
   * can be performed by TRUNCATE, DIFF or SNAPSHOT. The assumption is that
   * the server has all the committed transactions in its transaction log.
   *
   *  . If the epoch of the last transaction is different from the epoch of
   *  the last transaction of this server's. The whole log of the peer's will be
   *  truncated by sending SNAPSHOT message and then this server will
   *  synchronize its history to the peer.
   *
   *  . If the epoch of the last transaction of the peer and this server are
   *  the same, then this server will send DIFF or TRUNCATE to only synchronize
   *  or truncate the diff.
   */
  protected class SyncPeerTask {
    private final String peerId;
    private final Zxid peerLatestZxid;
    private final Zxid lastSyncZxid;
    private final ClusterConfiguration clusterConfig;

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

    public void run() throws IOException {
      LOG.debug("Starts synchronizing {}", peerId);
      Zxid syncPoint = null;
      Log log = persistence.getLog();
      Zxid snapZxid = persistence.getSnapshotZxid();
      File snapFile = persistence.getSnapshotFile();

      LOG.debug("Last peer zxid is {}, last sync zxid is {}, snapshot is {}",
                peerLatestZxid, lastSyncZxid, snapZxid);

      if (snapFile == null ||
          (snapZxid.compareTo(peerLatestZxid) <= 0 &&
           lastSyncZxid.getEpoch() == peerLatestZxid.getEpoch())) {
        // Starts the synchronization without using snapshot if any of the two
        // cases happens:
        //  1) There's no snapshot file.
        //  2) The peer's log is the superset of the current snapshot and the
        //     server and peer have the same epoch number in last appended zxid.
        if (lastSyncZxid.getEpoch() == peerLatestZxid.getEpoch()) {
          // If the peer has same epoch number as the server.
          if (lastSyncZxid.compareTo(peerLatestZxid) >= 0) {
            // Means peer's history is the prefix of the server's.
            LOG.debug("{}'s history >= {}'s, sending DIFF.", serverId, peerId);
            syncPoint = new Zxid(peerLatestZxid.getEpoch(),
                                 peerLatestZxid.getXid() + 1);
            Message diff = MessageBuilder.buildDiff(lastSyncZxid);
            sendMessage(peerId, diff);
          } else {
            // Means peer's history is the superset of the server's.
            LOG.debug("{}'s history < {}'s, sending TRUNCATE.", serverId,
                      peerId);
            // Doesn't need to synchronize anything, just truncate.
            syncPoint = null;
            Message trunc =
              MessageBuilder.buildTruncate(lastSyncZxid, lastSyncZxid);
            sendMessage(peerId, trunc);
          }
        } else {
          // They have different epoch numbers. Truncate all.
          LOG.debug("The last epoch of {} and {} are different, sending "
                    + "TRUNCATE to truncate whole log.", serverId, peerId);
          syncPoint = new Zxid(0, 0);
          Message trunc =
            MessageBuilder.buildTruncate(Zxid.ZXID_NOT_EXIST, lastSyncZxid);
          sendMessage(peerId, trunc);
        }
      } else {
        // Synchronizing peer with snapshot file.
        Message snapshot = MessageBuilder.buildSnapshot(lastSyncZxid, snapZxid);
        // Sends snapshot message.
        transport.send(peerId, snapshot);
        // Sends actuall snapshot file.
        transport.send(peerId, snapFile);
        if (snapZxid.compareTo(lastSyncZxid) == 0) {
          // If there's nothing after snapshot needs to by synchronized.
          syncPoint = null;
        } else {
          // The first transaction that needs to be synchronized to peer after
          // snapshot.
          syncPoint = new Zxid(snapZxid.getEpoch(), snapZxid.getXid() + 1);
        }
      }
      if (syncPoint != null) {
        try (Log.LogIterator iter = log.getIterator(syncPoint)) {
          while (iter.hasNext()) {
            Transaction txn = iter.next();
            if (txn.getZxid().compareTo(this.lastSyncZxid) > 0) {
              break;
            }
            Message prop = MessageBuilder.buildProposal(txn);
            sendMessage(peerId, prop);
          }
        }
      }
      // At the end of the synchronization, send COP.
      Message syncEnd = MessageBuilder.buildSyncEnd(this.clusterConfig);
      sendMessage(peerId, syncEnd);
    }
  }

  /**
   * Task that processes client's send/leave requests. It will be started
   * once Participant enters broadcasting phase.
   */
  protected class SendRequestTask implements Callable<Void> {
    private final Future<Void> ft;
    private final String leader;
    private volatile boolean stop = false;

    public SendRequestTask(String leader) {
      this.leader = leader;
      ExecutorService es =
          Executors.newSingleThreadExecutor(DaemonThreadFactory.FACTORY);
      this.ft = es.submit(this);
      es.shutdown();
    }

    public void shutdown() throws ExecutionException, InterruptedException {
      this.stop = true;
      // Release semaphore in case the thread is blocked on acuiqring of the
      // semaphore.
      semPendingReqs.release();
      this.ft.get();
      LOG.debug("SendRequestTask has been shut down.");
    }

    @Override
    public Void call() throws Exception {
      LOG.debug("SendRequestTask gets started.");
      try {
        BlockingQueue<MessageTuple> requestQueue
          = participantState.getRequestQueue();
        while (!stop) {
          MessageTuple tuple = requestQueue.poll(500, TimeUnit.MILLISECONDS);
          if (tuple == null) {
            continue;
          }
          // Blocks if the maximum pending requests have been reached.
          semPendingReqs.acquire();
          sendMessage(this.leader, tuple.getMessage());
        }
      } catch (Exception e) {
        LOG.error("Caught exception in SendRequest!");
        throw e;
      }
      return null;
    }
  }
}
