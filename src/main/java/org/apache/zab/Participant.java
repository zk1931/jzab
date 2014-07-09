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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.zab.QuorumZab.FailureCaseCallback;
import org.apache.zab.QuorumZab.StateChangeCallback;
import org.apache.zab.QuorumZab.TestState;
import org.apache.zab.proto.ZabMessage;
import org.apache.zab.proto.ZabMessage.AckEpoch;
import org.apache.zab.proto.ZabMessage.Message;
import org.apache.zab.proto.ZabMessage.NewEpoch;
import org.apache.zab.proto.ZabMessage.ProposedEpoch;
import org.apache.zab.proto.ZabMessage.Message.MessageType;
import org.apache.zab.transport.DummyTransport;
import org.apache.zab.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;

/**
 * Participant of a Zab ensemble.
 */
public class Participant implements Callable<Void>,
                                    Transport.Receiver,
                                    ServerState,
                                    Election.ElectionCallback {
  /**
   * Used for communication between nodes.
   */
  protected final Transport transport;

  /**
   * Callback interface for testing purpose.
   */
  protected StateChangeCallback stateChangeCallback = null;

  /**
   * Callback which will be called in different points of code path to simulate
   * different kinds of failure cases.
   */
  protected FailureCaseCallback failCallback = null;

  /**
   * Message queue. The receiving callback simply parses the message and puts
   * it in queue, it's up to leader/follower/election module to take out
   * the message.
   */
  protected final BlockingQueue<MessageTuple> messageQueue =
    new LinkedBlockingQueue<MessageTuple>();

  /**
   * The transaction log.
   */
  protected final Log log;

  /**
   * Configuration of Zab.
   */
  protected final ZabConfig config;

  /**
   * The file to store the last acknowledged epoch.
   */
  private final File fAckEpoch;

  /**
   * The file to store the last proposed epoch.
   */
  private final File fProposedEpoch;

  /**
   * State Machine callbacks.
   */
  protected StateMachine stateMachine = null;

  /**
   * Elected leader.
   */
  private String electedLeader = null;

  /**
   * Used for leader election.
   */
  private CountDownLatch leaderCondition;

  /**
   * Current State of QuorumZab.
   */
  private ZabState currentState = ZabState.LOOKING;

  private static final Logger LOG = LoggerFactory.getLogger(Participant.class);

  enum ZabState {
    LOOKING,
    FOLLOWING,
    LEADING
  }

  /**
   * A tuple holds both message and its source.
   */
  static class MessageTuple {
    private final String source;
    private final Message message;

    public MessageTuple(String source, Message message) {
      this.source = source;
      this.message = message;
    }

    String getSource() {
      return this.source;
    }

    Message getMessage() {
      return this.message;
    }
  }

  public Participant(ZabConfig config,
                     StateMachine stateMachine)
      throws IOException {
    this(stateMachine, null, null, new TestState(config));
  }

  Participant(StateMachine stateMachine,
              StateChangeCallback cb,
              FailureCaseCallback fcb,
              TestState initialState) throws IOException {
    this.config = new ZabConfig(initialState.prop);
    this.stateMachine = stateMachine;
    this.fAckEpoch = new File(config.getLogDir(), "AckEpoch");
    this.fProposedEpoch = new File(config.getLogDir(), "ProposedEpoch");

    MDC.put("serverId", this.config.getServerId());
    MDC.put("state", "looking");

    if (initialState.getLog() == null) {
      // Transaction log file.
      File logFile = new File(this.config.getLogDir(), "transaction.log");
      this.log = new SimpleLog(logFile);
    } else {
      this.log = initialState.getLog();
    }

    LOG.debug("Txn log file for {} is {} ", this.config.getServerId(),
                                            this.config.getLogDir());

    this.stateChangeCallback = cb;
    this.failCallback = fcb;
    if (initialState.getTransportMap() == null) {
      this.transport = new DummyTransport(this.config.getServerId(), this);
    } else {
      this.transport = new DummyTransport(this.config.getServerId(),
                                          this,
                                          initialState.getTransportMap());
    }
  }

  void send(ByteBuffer request) {
    if (this.currentState == ZabState.LOOKING) {
      throw new RuntimeException("QuorumZab in LOOKING state can't serve "
          + "request.");
    }
    Message msg = MessageBuilder.buildRequest(request);
    MessageTuple tuple = new MessageTuple(this.config.getServerId(), msg);
    this.messageQueue.add(tuple);
  }

  @Override
  public void onReceived(String source, ByteBuffer message) {
    byte[] buffer = null;

    try {
      // Parses it to protocol message.
      buffer = new byte[message.remaining()];
      message.get(buffer);
      Message msg = Message.parseFrom(buffer);

      if (LOG.isTraceEnabled()) {
        LOG.trace("Received message from {}: {} ",
                  source,
                  TextFormat.shortDebugString(msg));
      }

      // Puts the message in message queue.
      this.messageQueue.add(new MessageTuple(source, msg));

    } catch (InvalidProtocolBufferException e) {
      LOG.error("Exception when parse protocol buffer.", e);
      // Puts an invalid message to queue, it's up to handler to decide what
      // to do.
      Message msg = MessageBuilder.buildInvalidMessage(buffer);
      this.messageQueue.add(new MessageTuple(source, msg));
    }
  }

  @Override
  public void onDisconnected(String serverId) {
  }


  /**
   * Starts main logic of participant.
   */
  @Override
  public Void call() throws Exception {
    LOG.debug("Participant starts running.");
    Election electionAlg = new RoundRobinElection();
    while (true) {
      try {
        MDC.put("state", "looking");
        MDC.put("phase", "electing");
        this.currentState = ZabState.LOOKING;
        if (this.stateChangeCallback != null) {
          this.stateChangeCallback.electing();
        }

        startLeaderElection(electionAlg);
        waitLeaderElected();

        LOG.debug("Selected {} as prospective leader.",
                  this.electedLeader);

        if (this.electedLeader.equals(this.config.getServerId())) {
          MDC.put("state", "leading");
          lead();
        } else {
          MDC.put("state", "following");
          follow();
        }
      } catch (RuntimeException e) {
        LOG.warn("Caught an exception ", e);
      }
    }
  }

  /**
   * Gets the last acknowledged epoch.
   *
   * @return the last acknowledged epoch.
   * @throws IOException in case of IO failures.
   */
  int getAckEpochFromFile() throws IOException {
    try {
      int ackEpoch = FileUtils.readIntFromFile(this.fAckEpoch);
      return ackEpoch;
    } catch (FileNotFoundException e) {
      LOG.debug("File not exist, initialize acknowledged epoch to -1");
      return -1;
    } catch (IOException e) {
      LOG.error("IOException encountered when access acknowledged epoch");
      throw e;
    }
  }

  /**
   * Updates the last acknowledged epoch.
   *
   * @param ackEpoch the updated last acknowledged epoch.
   * @throws IOException in case of IO failures.
   */
  void setAckEpoch(int ackEpoch) throws IOException {
    FileUtils.writeIntToFile(ackEpoch, this.fAckEpoch);
  }

  /**
   * Gets the last proposed epoch.
   *
   * @return the last proposed epoch.
   * @throws IOException in case of IO failures.
   */
  int getProposedEpochFromFile() throws IOException {
    try {
      int pEpoch = FileUtils.readIntFromFile(this.fProposedEpoch);
      return pEpoch;
    } catch (FileNotFoundException e) {
      LOG.debug("File not exist, initialize acknowledged epoch to -1");
      return -1;
    } catch (IOException e) {
      LOG.error("IOException encountered when access acknowledged epoch");
      throw e;
    }
  }

  /**
   * Updates the last proposed epoch.
   *
   * @param pEpoch the updated last proposed epoch.
   * @throws IOException in case of IO failure.
   */
  void setProposedEpoch(int pEpoch) throws IOException {
    FileUtils.writeIntToFile(pEpoch, this.fProposedEpoch);
  }

  /**
   * Gets a message from the queue.
   *
   * @return a message tuple contains the message and its source.
   * @throws TimeoutException in case of timeout.
   * @throws InterruptedException it's interrupted.
   */
  MessageTuple getMessage() throws TimeoutException, InterruptedException {
    MessageTuple tuple = messageQueue.poll(config.getTimeout(),
                                           TimeUnit.MILLISECONDS);
    // Checks if timeout.
    if (tuple == null) {
      throw new TimeoutException("Timeout while waiting for the message.");
    }
    return tuple;
  }

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
  MessageTuple getExpectedMessage(MessageType type, String source)
      throws TimeoutException, InterruptedException {
    int startTime = (int) (System.nanoTime() / 1000000);
    // Waits until the expected message is received.
    while (true) {
      MessageTuple tuple = getMessage();
      String from = tuple.getSource();

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
                    tuple.getSource(),
                    TextFormat.shortDebugString(tuple.getMessage()));
        }
      }
    }
  }

  /**
   * Gets last received proposed epoch.
   */
  @Override
  public int getProposedEpoch() {
    try {
      return getProposedEpochFromFile();
    } catch (IOException e) {
      return -1;
    }
  }

  /**
   * Gets the size of the ensemble.
   */
  @Override
  public int getEnsembleSize() {
    return config.getEnsembleSize();
  }

  /**
   * Gets the server list.
   */
  @Override
  public List<String> getServerList() {
    return config.getPeers();
  }

  /**
   * Gets the minimal quorum size.
   *
   * @return the minimal quorum size
   */
  @Override
  public int getQuorumSize() {
    return getEnsembleSize() / 2 + 1;
  }

  /**
   * Broadcasts the message to all the peers.
   *
   * @param peers the destination peers.
   * @param message the message to be broadcasted.
   */
  void broadcast(Iterator<String> peers, Message message) {
    transport.broadcast(peers, ByteBuffer.wrap(message.toByteArray()));
  }

  /**
   * Sends message to the specific destination.
   *
   * @param dest the destination of the message.
   * @param message the message to be sent.
   */
  void sendMessage(String dest, Message message) {
    LOG.trace("Sends to {} message {}",
              dest,
              TextFormat.shortDebugString(message));
    this.transport.send(dest, ByteBuffer.wrap(message.toByteArray()));
  }

  /* -----------------   For ELECTING state only -------------------- */


  void startLeaderElection(Election electionAlg) {
    this.leaderCondition = new CountDownLatch(1);
    electionAlg.initialize(this, this);
  }

  @Override
  public void leaderElected(String serverId) {
    this.electedLeader = serverId;
    this.leaderCondition.countDown();
  }

  void waitLeaderElected() throws InterruptedException {
    this.leaderCondition.await();
  }


  /* -----------------   For BOTH LEADING and FOLLOWING -------------------- */

  /**
   * Waits synchronization message from peer. This method is called on both
   * leader side and follower side.
   *
   * @param peer the id of the expected peer that synchronization message will
   * come from.
   */
  void waitForSync(String peer)
      throws InterruptedException, TimeoutException, IOException {
    LOG.debug("Waiting sync from {}.", peer);
    Zxid lastZxid = this.log.getLatestZxid();
    // The last zxid of peer.
    Zxid lastZxidPeer = null;
    Message msg = null;
    String source = null;
    // Expects getting message of DIFF or TRUNCATE or SNAPSHOT or PULL_TXN_REQ
    // from elected leader.
    while (true) {
      MessageTuple tuple = getMessage();
      source = tuple.getSource();
      msg = tuple.getMessage();
      if ((msg.getType() != MessageType.DIFF &&
           msg.getType() != MessageType.TRUNCATE &&
           msg.getType() != MessageType.SNAPSHOT &&
           msg.getType() != MessageType.PULL_TXN_REQ) ||
          !source.equals(peer)) {
        LOG.debug("Got unexpected message {} from {}.",
                  TextFormat.shortDebugString(msg),
                  source);
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
      lastZxidPeer = new Zxid(z.getEpoch(), z.getXid());
      // Synchronize its history to leader.
      SyncPeerTask syncTask = new SyncPeerTask(source, lastZxidPeer);
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
      return;
    }

    if (msg.getType() == MessageType.DIFF) {
      // DIFF message.
      LOG.debug("Got DIFF : {}",
                TextFormat.shortDebugString(msg));
      ZabMessage.Diff diff = msg.getDiff();
      // Remember last zxid of the peer.
      lastZxidPeer = MessageBuilder.fromProtoZxid(diff.getLastZxid());
      if(lastZxid.compareTo(lastZxidPeer) == 0) {
        // Means the two nodes have exact same history.
        return;
      }
    } else if (msg.getType() == MessageType.TRUNCATE) {
      // TRUNCATE message.
      LOG.debug("Got TRUNCATE: {}",
                TextFormat.shortDebugString(msg));
      ZabMessage.Truncate trunc = msg.getTruncate();
      Zxid lastPrefixZxid = MessageBuilder
                            .fromProtoZxid(trunc.getLastPrefixZxid());
      this.log.truncate(lastPrefixZxid);
      return;
    } else {
      // SNAPSHOT message.
      LOG.debug("Got SNAPSHOT: {}",
                TextFormat.shortDebugString(msg));
      ZabMessage.Snapshot snap = msg.getSnapshot();
      lastZxidPeer = MessageBuilder.fromProtoZxid(snap.getLastZxid());
      this.log.truncate(Zxid.ZXID_NOT_EXIST);

      // Check if the history of peer is empty.
      if (lastZxidPeer.compareTo(Zxid.ZXID_NOT_EXIST) == 0) {
        return;
      }
    }
    // Get subsequent proposals.
    while (true) {
      MessageTuple tuple = getExpectedMessage(MessageType.PROPOSAL, peer);
      msg = tuple.getMessage();
      source = tuple.getSource();

      LOG.debug("Got PROPOSAL from {} : {}",
                source,
                TextFormat.shortDebugString(msg));

      ZabMessage.Proposal prop = msg.getProposal();
      Zxid zxid = MessageBuilder.fromProtoZxid(prop.getZxid());
      // Accept the proposal.
      this.log.append(MessageBuilder.fromProposal(prop));

      // Check if this is the last proposal.
      if (zxid.compareTo(lastZxidPeer) == 0) {
        return;
      }
    }
  }


  /**
   * Returns all the transactions appear in log.
   *
   * @return a list of transactions.
   */
  List<Transaction> getAllTxns() throws IOException {
    List<Transaction> txns = new ArrayList<Transaction>();

    try(Log.LogIterator iter = this.log.getIterator(new Zxid(0, 0))) {
      while(iter.hasNext()) {
        txns.add(iter.next());
      }
      return txns;
    }
  }


  /* -----------------   For LEADING state only -------------------- */

  /**
   * Begins executing leader steps. It returns if any exception is caught,
   * which causes it goes back to election phase.
   */
  void lead() {

    Map<String, PeerHandler> quorumSet =
        new ConcurrentHashMap<String, PeerHandler>();

    ExecutorService es = Executors
                         .newCachedThreadPool(DaemonThreadFactory.FACTORY);

    try {

      /* -- Discovering phase -- */
      MDC.put("phase", "discovering");
      LOG.debug("Now it's in discovering phase.");

      if (stateChangeCallback != null) {
        stateChangeCallback.leaderDiscovering(config.getServerId());
      }

      if (failCallback != null) {
        failCallback.leaderDiscovering();
      }

      getPropsedEpochFromQuorum(quorumSet);
      proposeNewEpoch(quorumSet);
      waitEpochAckFromQuorum(quorumSet);

      LOG.debug("Established new epoch {}!",
                getProposedEpochFromFile());

      // Finds one who has the "best" history.
      String serverId = selectSyncHistoryOwner(quorumSet);
      LOG.debug("Chooses {} to pull its history.",
                serverId);


      /* -- Synchronizing phase -- */
      MDC.put("phase", "synchronizing");
      LOG.debug("It's in synchronizating phase.");

      if (stateChangeCallback != null) {
        stateChangeCallback.leaderSynchronizating(getProposedEpochFromFile());
      }

      if (failCallback != null) {
        failCallback.leaderSynchronizing();
      }

      if (!serverId.equals(this.config.getServerId())) {
        // Pulls history from the follower.
        synchronizeFromFollower(serverId);
      }
      // Updates ACK EPOCH of leader.
      setAckEpoch(getProposedEpochFromFile());
      beginSynchronizing(quorumSet, es);
      waitNewLeaderAckFromQuorum(quorumSet);


      /* -- Broadcasting phase -- */
      MDC.put("phase", "broadcast");
      LOG.debug("Now it's in broadcasting phase.");

      if (failCallback != null) {
        failCallback.leaderBroadcasting();
      }

      if (stateChangeCallback != null) {
        stateChangeCallback.leaderBroadcasting(getAckEpochFromFile(),
                                               getAllTxns());
      }

      this.currentState = ZabState.LEADING;
      beginBroadcasting(quorumSet, es);

    } catch (InterruptedException | TimeoutException | IOException |
        RuntimeException e) {
      LOG.error("Caught exception", e);
      // Shutdown all the PeerHandlers in quorum set.
      for (PeerHandler ph : quorumSet.values()) {
        ph.shutdown();
      }
      es.shutdown();
    }
  }

  /**
   * Waits until receives the CEPOCH message from the quorum.
   *
   * @param quorumSet the quorum set.
   * @throws InterruptedException if anything wrong happens.
   * @throws TimeoutException in case of timeout.
   */
  void getPropsedEpochFromQuorum(Map<String, PeerHandler> quorumSet)
      throws InterruptedException, TimeoutException {
    // Gets last proposed epoch from other servers (not including leader).
    while (quorumSet.size() < getQuorumSize() - 1) {
      MessageTuple tuple = getExpectedMessage(MessageType.PROPOSED_EPOCH, null);
      Message msg = tuple.getMessage();
      String source = tuple.getSource();
      ProposedEpoch epoch = msg.getProposedEpoch();

      if (quorumSet.containsKey(source)) {
        throw new RuntimeException("Quorum set has already contained "
            + source + ", probably a bug?");
      }

      PeerHandler ph = new PeerHandler(source,
                                       this.transport,
                                       this.config.getTimeout() / 3);

      ph.setLastProposedEpoch(epoch.getProposedEpoch());
      quorumSet.put(source, ph);
    }
    LOG.debug("Got proposed epoch from a quorum.");
  }

  /**
   * Finds an epoch number which is higher than any proposed epoch in quorum
   * set and propose the epoch to them.
   *
   * @param quorumSet the quorum set.
   * @throws IOException in case of IO failure.
   */
  void proposeNewEpoch(Map<String, PeerHandler> quorumSet)
      throws IOException {
    List<Integer> epochs = new ArrayList<Integer>();

    // Puts leader's last received proposed epoch in list.
    epochs.add(getProposedEpochFromFile());

    for (PeerHandler stat : quorumSet.values()) {
      epochs.add(stat.getLastProposedEpoch());
    }

    int newEpoch = Collections.max(epochs) + 1;

    // Updates leader's last proposed epoch.
    setProposedEpoch(newEpoch);

    LOG.debug("Begins proposing new epoch {}",
              newEpoch);

    // Sends new epoch message to quorum.
    broadcast(quorumSet.keySet().iterator(),
              MessageBuilder.buildNewEpochMessage(newEpoch));
  }

  /**
   * Waits until the new epoch is established.
   *
   * @param quorumSet the quorum set.
   * @throws InterruptedException if anything wrong happens.
   * @throws TimeoutException in case of timeout.
   */
  void waitEpochAckFromQuorum(Map<String, PeerHandler> quorumSet)
      throws InterruptedException, TimeoutException {

    int ackCount = 0;

    // Waits the Ack from all other peers in the quorum set.
    while (ackCount < quorumSet.size()) {
      MessageTuple tuple = getExpectedMessage(MessageType.ACK_EPOCH, null);
      Message msg = tuple.getMessage();
      String source = tuple.getSource();

      if (!quorumSet.containsKey(source)) {
        LOG.warn("The Epoch ACK comes from {} who is not in quorum set, "
                 + "possibly from previous epoch?",
                 source);
        continue;
      }

      ackCount++;

      AckEpoch ackEpoch = msg.getAckEpoch();
      ZabMessage.Zxid zxid = ackEpoch.getLastZxid();

      // Updates follower's f.a and lastZxid.
      PeerHandler ph = quorumSet.get(source);
      ph.setLastAckedEpoch(ackEpoch.getAcknowledgedEpoch());
      ph.setLastZxid(MessageBuilder.fromProtoZxid(zxid));
    }

    LOG.debug("Received ACKs from the quorum set of size {}.",
              quorumSet.size() + 1);
  }

  /**
   * Finds a server who has the largest acknowledged epoch and longest
   * history.
   *
   * @param quorumSet the quorum set.
   * @return the id of the server
   * @throws IOException
   */
  String selectSyncHistoryOwner(Map<String, PeerHandler> quorumSet)
      throws IOException {
    // L.1.2 Select the history of a follwer f to be the initial history
    // of the new epoch. Follwer f is such that for every f' in the quorum,
    // f'.a < f.a or (f'.a == f.a && f'.zxid <= f.zxid).

    // Get the acknowledged epoch and the latest zxid of the leader.
    int ackEpoch = getAckEpochFromFile();
    Zxid zxid = log.getLatestZxid();
    String serverId = config.getServerId();

    Iterator<Map.Entry<String, PeerHandler>> iter;
    iter = quorumSet.entrySet().iterator();

    while (iter.hasNext()) {
      Map.Entry<String, PeerHandler> entry = iter.next();

      int fEpoch = entry.getValue().getLastAckedEpoch();
      Zxid fZxid = entry.getValue().getLastZxid();

      if (fEpoch > ackEpoch ||
          (fEpoch == ackEpoch && fZxid.compareTo(zxid) > 0)) {
        ackEpoch = fEpoch;
        zxid = fZxid;
        serverId = entry.getKey();
      }
    }

    LOG.debug("{} has largest acknowledged epoch {} and longest history {}",
              serverId, ackEpoch, zxid);

    if (this.stateChangeCallback != null) {
      this.stateChangeCallback.initialHistoryOwner(serverId,
                                                   ackEpoch,
                                                   zxid);
    }
    return serverId;
  }

  /**
   * Pulls the history from the server who has the "best" history.
   *
   * @param serverId the id of the server whose history is selected.
   */
  void synchronizeFromFollower(String serverId)
      throws IOException, TimeoutException, InterruptedException {

    LOG.debug("Begins synchronizing from follower {}.",
              serverId);

    Zxid lastZxid = log.getLatestZxid();
    Message pullTxn = MessageBuilder.buildPullTxnReq(lastZxid);
    LOG.debug("Last zxid of {} is {}", this.config.getServerId(),
                                       lastZxid);

    sendMessage(serverId, pullTxn);
    // Waits until the synchronization is finished.
    waitForSync(serverId);
  }

  /**
   * Waits for synchronization to followers complete.
   *
   * @param quorumSet the followers need to be wait.
   * @throws TimeoutException in case of timeout.
   * @throws InterruptedException in case of interrupt.
   */
  void waitNewLeaderAckFromQuorum(Map<String, PeerHandler> quorumSet)
      throws TimeoutException, InterruptedException, IOException {

    LOG.debug("Waiting for synchronization to followers complete.");

    int completeCount = 0;

    while (completeCount < quorumSet.size()) {
      MessageTuple tuple = getExpectedMessage(MessageType.ACK, null);
      ZabMessage.Ack ack = tuple.getMessage().getAck();
      String source =tuple.getSource();

      Zxid zxid = MessageBuilder.fromProtoZxid(ack.getZxid());

      if (zxid.compareTo(this.log.getLatestZxid()) != 0) {
        LOG.error("The follower {} is not correctly synchronized.", source);

        throw new RuntimeException("The synchronized follower's last zxid"
            + "doesn't match last zxid of current leader.");
      }

      if (!quorumSet.containsKey(source)) {
        LOG.warn("Quorum set doesn't contain {}, a bug?", source);
        continue;
      }
      completeCount++;
    }
  }

  /**
   * Starts synchronizing peers in background threads.
   *
   * @param quorumSet the quorum set.
   * @param es the ExecutorService used to running synchronizing tasks.
   * @throws IOException in case of IO failure.
   */
  void beginSynchronizing(Map<String, PeerHandler> quorumSet,
                          ExecutorService es) throws IOException {
    // Synchronization is performed in other threads.
    for (PeerHandler ph : quorumSet.values()) {
      ph.setSyncTask(new SyncPeerTask(ph.getServerId(),
                                      ph.getLastZxid()));
      ph.setNewLeaderEpoch(getProposedEpochFromFile());
      ph.setFuture(es.submit(ph));
    }
  }

  /**
   * Entering broadcasting phase, leader broadcasts proposal to
   * followers.
   *
   * @param quorumSet the quorum set. Now it includes leader itself.
   * @throws InterruptedException if it's interrupted.
   * @throws TimeoutException in case of timeout.
   * @throws IOException in case of IO failure.
   */
  void beginBroadcasting(Map<String, PeerHandler> quorumSet,
                         ExecutorService es)
      throws TimeoutException, InterruptedException, IOException {
    int currentEpoch = getAckEpochFromFile();
    Zxid nextZxid = new Zxid(currentEpoch, 0);
    PreProcessor preproc = new PreProcessor(this.stateMachine,
                                            nextZxid,
                                            quorumSet);
    AckProcessor ackProcessor = new AckProcessor(quorumSet, getQuorumSize());
    // Adds leader itself to quorum set.
    SyncProposalProcessor syncProcessor =
        new SyncProposalProcessor(this.log,
                                  this.transport);
    CommitProcessor commitProcessor =
        new CommitProcessor(this.log,
                            this.stateMachine,
                            this.config.getServerId());
    ShortCircuitTransport scTransport =
        new ShortCircuitTransport(syncProcessor,
                                  commitProcessor,
                                  this.messageQueue);
    // Adds itself in quorumSet.
    PeerHandler lh = new PeerHandler(this.config.getServerId(),
                                     scTransport,
                                     this.config.getTimeout() / 3);
    lh.setFuture(es.submit(lh));
    quorumSet.put(this.config.getServerId(), lh);
    // Sends commit message to commit all the transactions proposed in
    // synchronizing phase.
    Zxid lastZxid = this.log.getLatestZxid();
    Message commit = MessageBuilder.buildCommit(lastZxid);
    queueAllPeers(commit, quorumSet);

    try {
      while (quorumSet.size() >= getQuorumSize()) {
        /*
         * Starts broadcasting loop, the leader will handle 5 kinds of messages
         * in the loop.
         *
         *  1) PROPOSED_EPOCH : The new follower may join the quorum set in
         *     broadcasting phase of leader. The leader will send back the
         *     established epoch number (NEW_EPOCH) to follower.
         *
         *  2) ACK_EPOCH : Once the follower receives NEW_EPOCH from leader,
         *     followers will send back ACK_EPOCH message, the leader will add
         *     the follower to quorum set and starts synchronizing in
         *     background thread.
         *
         *  3) REQUEST : The follower or client may send REQUEST to leader,
         *     once the leader receives REQUEST, it will hand it to
         *     PreProcessor to convert it into transaction and broadcast to
         *     all the followers.
         *
         *  4) ACK : Once followers accept the proposal, they will send ACK to
         *     leader, leader will hand it to AckProcessor to find out the
         *     potential newly committed transactions and notify followers with
         *     the COMMIT message.
         *
         *  5) HEARTBEAT : The leader will send HEARTBEAT message to followers
         *     periodically, and followers will reply HEARTBEAT message. Once
         *     the leader receive the HEARTBEAT message, it will update the
         *     followers' last replied HEAERTBEAT time in order to mark them as
         *     alive.
         */
        MessageTuple tuple = getMessage();
        Message msg = tuple.getMessage();
        String source = tuple.getSource();

        if (msg.getType() == MessageType.PROPOSED_EPOCH) {
          LOG.debug("Leader {} got PROPOSED_EPOCH from {}.",
                    this.config.getServerId(),
                    source);
          // Sends NEW_EPOCH message.
          Message newEpoch = MessageBuilder.buildNewEpochMessage(currentEpoch);
          sendMessage(source, newEpoch);
        } else if (msg.getType() == MessageType.ACK_EPOCH) {
          LOG.debug("Leader {} got ACK_EPOCH from {}",
                    this.config.getServerId(),
                    source);
          AckEpoch ackEpoch = msg.getAckEpoch();
          Zxid zxid = MessageBuilder.fromProtoZxid(ackEpoch.getLastZxid());
          // Updates follower's f.a and lastZxid.
          PeerHandler ph = new PeerHandler(source,
                                           this.transport,
                                           this.config.getTimeout() / 3);
          ph.setLastZxid(zxid);
          ph.setNewLeaderEpoch(currentEpoch);
          ph.setSyncTask(new SyncPeerTask(source, zxid));
          quorumSet.put(source, ph);
          // Starts background thread for synchronizing.
          ph.setFuture(es.submit(ph));
        } else {
          // In broadcasting phase, the only expected messages come outside
          // the quorum set is PROPOSED_EPOCH and ACK_EPOCH.
          if (!quorumSet.containsKey(source)) {
            LOG.debug("Got message {} from {} outside quorum.",
                      TextFormat.shortDebugString(msg),
                      source);
            continue;
          }
          if (msg.getType() == MessageType.ACK) {
            // Got ACK from peers.
            LOG.debug("Got ACK {} from {}.",
                      MessageBuilder.fromProtoZxid(msg.getAck().getZxid()),
                      source);
            ackProcessor.processRequest(new Request(source, msg));
          } else if (msg.getType() == MessageType.REQUEST) {
            // Got REQUEST from user or other peers.
            LOG.debug("Got REQUEST from {}.",
                      source);
            preproc.processRequest(new Request(this.config.getServerId(), msg));
          } else if (msg.getType() == MessageType.HEARTBEAT) {
            // Got HEARTBEAT message.
            LOG.trace("Got HEARTBEAT replies from {}",
                      source);
          } else {
            LOG.warn("Unexpected messgae : {} from {}",
                      TextFormat.shortDebugString(msg),
                      source);
          }
          // Updates last received message time for this follower.
          quorumSet.get(source).updateHeartbeatTime();
          // Checks if any peers in quorum set are dead.
          checkFollowerLiveness(quorumSet);
        }
      }

      LOG.debug("Detects the size of the ensemble is less than the"
          + "quorum size {}, goes back to electing phase.",
          getQuorumSize());

    } finally {
      // Stop all the processors.
      try {
        ackProcessor.shutdown();
        preproc.shutdown();
        commitProcessor.shutdown();
        syncProcessor.shutdown();
      } catch (ExecutionException e) {
        LOG.error("Caught exectuion exception.",
                  e);
      }
    }
  }

  void queueAllPeers(Message commit, Map<String, PeerHandler> quorumSet) {
    for (PeerHandler ph : quorumSet.values()) {
      ph.queueMessage(commit);
    }
  }

  void checkFollowerLiveness(Map<String, PeerHandler> quorumSet) {
    long currentTime = System.nanoTime();
    long timeoutNs = this.config.getTimeout() * (long)1000000;
    for (PeerHandler ph : quorumSet.values()) {
      if (ph.getServerId().equals(this.config.getServerId())) {
        continue;
      }
      if (currentTime - ph.getLastHeartbeatTime() >= timeoutNs) {
        // Removes the peer who is likely to be dead.
        ph.shutdown();
        quorumSet.remove(ph.getServerId());
        LOG.warn("Peer {} is likely to be dead, remove it from quorum set.",
                 ph.getServerId());
      }
    }
  }

  /* -----------------   For FOLLOWING state only -------------------- */

  /**
   * Begins executing follower steps. It returns if any exception is caught,
   * which causes it goes back to election phase.
   */
  void follow() {

    try {

      /* -- Discovering phase -- */
      MDC.put("phase", "discovering");
      LOG.debug("It's in discovering phase.");

      if (stateChangeCallback != null) {
        stateChangeCallback.followerDiscovering(this.electedLeader);
      }

      if (failCallback != null) {
        failCallback.followerDiscovering();
      }

      sendProposedEpoch();

      receiveNewEpoch();


      /* -- Synchronizing phase -- */
      MDC.put("phase", "synchronizing");
      LOG.debug("Now it's in synchronizating phase.");

      if (stateChangeCallback != null) {
        stateChangeCallback.followerSynchronizating(getProposedEpochFromFile());
      }

      if (failCallback != null) {
        failCallback.followerSynchronizing();
      }

      waitForSync(this.electedLeader);

      waitNewLeaderMesage();


      /* -- Broadcasting phase -- */
      MDC.put("phase", "broadcast");
      LOG.debug("Now it's in broadcasting phase.");

      if (stateChangeCallback != null) {
        stateChangeCallback.followerBroadcasting(getAckEpochFromFile(),
                                                 getAllTxns());
      }

      if (failCallback != null) {
        failCallback.followerBroadcasting();
      }

      this.currentState = ZabState.FOLLOWING;
      beginAccepting();

    } catch (InterruptedException | TimeoutException | IOException |
        RuntimeException e) {
      LOG.error("Caught exception.", e);
    }
  }

  /**
   * Sends CEPOCH message to its prospective leader.
   * @throws IOException in case of IO failure.
   */
  void sendProposedEpoch() throws IOException {
    Message message = MessageBuilder
                      .buildProposedEpoch(getProposedEpochFromFile());
    sendMessage(this.electedLeader, message);
  }

  /**
   * Waits until receives the NEWEPOCH message from leader.
   *
   * @throws InterruptedException if anything wrong happens.
   * @throws TimeoutException in case of timeout.
   * @throws IOException in case of IO failure.
   */
  void receiveNewEpoch()
      throws InterruptedException, TimeoutException, IOException {

    MessageTuple tuple = getExpectedMessage(MessageType.NEW_EPOCH,
                                            this.electedLeader);

    Message msg = tuple.getMessage();
    String source = tuple.getSource();
    NewEpoch epoch = msg.getNewEpoch();

    if (epoch.getNewEpoch() < getProposedEpochFromFile()) {
      LOG.error("New epoch {} from {} is smaller than last received "
                + "proposed epoch {}",
                epoch.getNewEpoch(),
                source,
                getProposedEpochFromFile());

      throw new RuntimeException("New epoch is smaller than current one.");
    }

    // Updates follower's last proposed epoch.
    setProposedEpoch(epoch.getNewEpoch());

    LOG.debug("Received the new epoch proposal {} from {}.",
              epoch.getNewEpoch(),
              source);

    Zxid zxid = log.getLatestZxid();
    // Sends ACK to leader.
    sendMessage(this.electedLeader,
                MessageBuilder.buildAckEpoch(getAckEpochFromFile(),
                                             zxid));
  }

  /**
   * Waits for NEW_LEADER message and sends back ACK and update ACK epoch.
   *
   * @throws TimeoutException in case of timeout.
   * @throws InterruptedException in case of interrupt.
   * @throws IOException in case of IO failure.
   */
  void waitNewLeaderMesage()
      throws TimeoutException, InterruptedException, IOException {

    LOG.debug("{} is waiting for New Leader message from {}.",
              this.config.getServerId(),
              this.electedLeader);

    MessageTuple tuple = getExpectedMessage(MessageType.NEW_LEADER,
                                            this.electedLeader);
    Message msg = tuple.getMessage();
    String source = tuple.getSource();

    LOG.debug("Got NEW_LEADER message from {} : {}.",
              source,
              TextFormat.shortDebugString(msg));

    // NEW_LEADER message.
    ZabMessage.NewLeader nl = msg.getNewLeader();
    int epoch = nl.getEpoch();
    // Sync Ack epoch to disk.
    setAckEpoch(epoch);
    this.log.sync();

    Message ack = MessageBuilder.buildAck(this.log.getLatestZxid());
    sendMessage(source, ack);
  }

  /**
   * Entering broadcasting phase.
   *
   * @throws InterruptedException if it's interrupted.
   * @throws TimeoutException  in case of timeout.
   * @throws IOException in case of IOException.
   */
  void beginAccepting()
      throws TimeoutException, InterruptedException, IOException {

    SyncProposalProcessor syncProcessor =
        new SyncProposalProcessor(this.log, this.transport);

    CommitProcessor commitProcessor =
        new CommitProcessor(log, stateMachine, this.config.getServerId());

    // The last time of HEARTBEAT message comes from leader.
    long lastHeartbeatTime = System.nanoTime();

    try {
      while (true) {
        /*
         * Starts accepting loop, the follower will handle 4 kinds of messages
         * in the loop.
         *
         *  1) PROPOSAL : The leader will propose proposals to followers,
         *  followers will hand it to SyncProposalProcessor to accept it and
         *  send back ACK to leader.
         *
         *  2) COMMIT : Once a quorum of followers accept the proposal, the
         *  leader will send COMMIT message to all the peers in quorum set.
         *  Followers will hand in to DeliverProcesor to deliver it to client
         *  application.
         *
         *  3) REQUEST : The client may send REQUEST to followers, followers
         *  simply forward the request to leader.
         *
         *  4) HEARTBEAT : Leader will send HEARTBEAT messages to followers
         *  periodically, followers will reply the same message to leader.
         */
        MessageTuple tuple = getMessage();
        Message msg = tuple.getMessage();
        String source = tuple.getSource();

        // The follower only expect receiving message from leader and
        // itself(REQUEST).
        if (source.equals(this.electedLeader)) {
          lastHeartbeatTime = System.nanoTime();
        } else {
          // Checks if the leader is alive.
          long timeDiff = (System.nanoTime() - lastHeartbeatTime) / 1000000;
          if ((int)timeDiff >= this.config.getTimeout()) {
            // HEARTBEAT timeout.
            LOG.warn("Detects there's a timeout in waiting"
                + "message from leader {}, goes back to leader electing",
                this.electedLeader);
            throw new TimeoutException("HEARTBEAT timeout!");
          }
          if (!source.equals(this.config.getServerId())) {
            LOG.debug("Got unexpected message from {}, ignores.",
                      source);
            continue;
          }
        }

        if (msg.getType() == MessageType.PROPOSAL) {
          LOG.debug("Got PROPOSAL {}.",
                    MessageBuilder.fromProtoZxid(msg.getProposal().getZxid()));
          Transaction txn = MessageBuilder.fromProposal(msg.getProposal());
          Zxid zxid = txn.getZxid();
          if (zxid.getEpoch() == getAckEpochFromFile()) {
            // Dispatch to SyncProposalProcessor.
            syncProcessor.processRequest(new Request(this.electedLeader, msg));
          } else {
            LOG.debug("The proposal has the wrong epoch number {}.",
                      zxid.getEpoch());
            throw new RuntimeException("The proposal has wrong epoch number.");
          }
        } else if (msg.getType() == MessageType.COMMIT) {
          LOG.debug("Got COMMIT {} from {}.",
                    MessageBuilder.fromProtoZxid(msg.getCommit().getZxid()),
                    source);
          commitProcessor.processRequest(new Request(source, msg));
        } else if (msg.getType() == MessageType.REQUEST) {
          LOG.debug("Got REQUEST from {}.",
                    source);
          // Forwards REQUEST to the leader.
          sendMessage(this.electedLeader, msg);
        } else if (msg.getType() == MessageType.HEARTBEAT) {
          LOG.trace("Got HEARTBEAT from {}.",
                    source);
          // Replies HEARTBEAT message to leader.
          Message heartbeatReply = MessageBuilder.buildHeartbeat();
          sendMessage(source, heartbeatReply);
        } else {
          LOG.warn("Unexpected messgae : {} from {}",
                    TextFormat.shortDebugString(msg),
                    source);
        }
      }
    } finally {
      try {
        commitProcessor.shutdown();
        syncProcessor.shutdown();
      } catch (ExecutionException e) {
        LOG.error("Follower {} caught execution exception.", e);
      }
    }
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
  class SyncPeerTask {
    private final String peerId;
    private final Zxid peerLatestZxid;

   /**
    * Constructs the SyncPeerTask object.
    *
    * @param peerId the id of the peer.
    * @param peerLatestZxid the last zxid of the peer.
    */
    public SyncPeerTask(String peerId, Zxid peerLatestZxid) {
      this.peerId = peerId;
      this.peerLatestZxid = peerLatestZxid;
    }

    public void run() throws IOException {
      LOG.debug("Begins synchronizing history to {}(last zxid : {})",
                peerId,
                peerLatestZxid);

      Zxid lastZxid = log.getLatestZxid();
      Zxid syncPoint = null;

      if (lastZxid.getEpoch() == peerLatestZxid.getEpoch()) {
        // If the peer has same epoch number as the server.

        if (lastZxid.compareTo(peerLatestZxid) >= 0) {
          // Means peer's history is the prefix of the server's.
          LOG.debug("{}'s history is >= {}'s, sending DIFF.",
                    config.getServerId(),
                    peerId);

          syncPoint = new Zxid(peerLatestZxid.getEpoch(),
                               peerLatestZxid.getXid() + 1);

          Message diff = MessageBuilder.buildDiff(lastZxid);
          sendMessage(peerId, diff);

        } else {
          // Means peer's history is the superset of the server's.
          LOG.debug("{}'s history is < {}'s, sending TRUNCATE.",
                    config.getServerId(),
                    peerId);

          syncPoint = new Zxid(lastZxid.getEpoch(), lastZxid.getXid() + 1);
          Message trunc = MessageBuilder.buildTruncate(lastZxid);
          sendMessage(peerId, trunc);
        }
      } else {
        // They have different epoch numbers. Truncate all.
        LOG.debug("The last epoch of {} and {} are different, sending "
                  + "SNAPSHOT.",
                  config.getServerId(),
                  peerId);

        syncPoint = new Zxid(0, 0);
        Message snapshot = MessageBuilder.buildSnapshot(lastZxid);
        sendMessage(peerId, snapshot);
      }

      try (Log.LogIterator iter = log.getIterator(syncPoint)) {
        while (iter.hasNext()) {
          Transaction txn = iter.next();
          Message prop = MessageBuilder.buildProposal(txn);
          sendMessage(peerId, prop);
        }
      }
    }
  }
}
