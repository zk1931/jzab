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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
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
import org.apache.zab.transport.NettyTransport;
import org.apache.zab.transport.Transport;
import org.apache.zab.Zab.State;
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
   * Maximum batch size for SyncRequestProcessor.
   *
   * TODO We might want to expose this setting to the user.
   */
  protected static final int SYNC_MAX_BATCH_SIZE = 1000;

  /**
   * The file to store the last acknowledged epoch.
   */
  private final File fAckEpoch;

  /**
   * The file to store the last proposed epoch.
   */
  private final File fProposedEpoch;

  /**
   * The file to store the last proposed configuration.
   */
  private final File fProposedConfig;

  /**
   * The file to store the last acknowledged config.
   */
  private final File fAckConfig;

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
  private State currentState = State.LOOKING;

  /**
   * Used to store the quorum set.
   */
  private Map<String, PeerHandler> quorumSet =
      new ConcurrentHashMap<String, PeerHandler>();

  /**
   * The last delivered zxid. It's not a persisten variable and gets
   * initialized to (0, -1) everytime it gets started.
   */
  private Zxid lastDeliveredZxid = Zxid.ZXID_NOT_EXIST;

  /**
   * If it's in broadcasting phase.
   */
  private Boolean isBroadcasting = false;

  private static final Logger LOG = LoggerFactory.getLogger(Participant.class);

  /**
   * A tuple holds both message and its source.
   */
  static class MessageTuple {
    private final String source;
    private final Message message;
    // Special tuple forces Participant goes back to leader election.
    static final MessageTuple GO_BACK = new MessageTuple(null, null);

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

  /**
   * This exception is only used to force leader/follower goes back to election
   * phase.
   */
  static class BackToElectionException extends RuntimeException {
  }

  static class Configuration {
    private final Zxid version;
    private final List<String> peers;
    private final String serverId;

    public Configuration(Zxid version, List<String> peers, String serverId) {
      this.version = version;
      this.peers = peers;
      this.serverId = serverId;
    };

    public Zxid getVersion() {
      return this.version;
    }

    public List<String> getPeers() {
      return this.peers;
    }

    public String getServerId() {
      return this.serverId;
    }

    public Properties toProperties() {
      Properties prop = new Properties();
      StringBuilder strBuilder = new StringBuilder();
      String strVersion = this.version.getEpoch() + " " + this.version.getXid();
      for (String peer : this.peers) {
        strBuilder.append(peer + ";");
      }
      prop.setProperty("peers", strBuilder.toString());
      prop.setProperty("version", strVersion);
      prop.setProperty("serverId", this.serverId);
      return prop;
    }

    public static Configuration fromProperties(Properties prop) {
      String strPeers = prop.getProperty("peers");
      String[] strVersion = prop.getProperty("version").split(" ");
      String serverId = prop.getProperty("serverId");
      List<String> peerList = Arrays.asList(strPeers.split(";"));
      if (strVersion.length != 2) {
        LOG.error("Configuration file has wrong format of version!");
        throw new RuntimeException("Configuration has wrong format of version");
      }
      Zxid version = new Zxid(Integer.parseInt(strVersion[0]),
                              Integer.parseInt(strVersion[1]));
      return new Configuration(version, peerList, serverId);
    }
  }

  public Participant(ZabConfig config,
                     StateMachine stateMachine)
      throws IOException, InterruptedException {
    this(stateMachine, null, null, new TestState(config));
  }

  Participant(StateMachine stateMachine,
              StateChangeCallback cb,
              FailureCaseCallback fcb,
              TestState initialState) throws
              IOException, InterruptedException {
    this.config = new ZabConfig(initialState.prop);
    this.stateMachine = stateMachine;
    this.fAckEpoch = new File(config.getLogDir(), "AckEpoch");
    this.fProposedEpoch = new File(config.getLogDir(), "ProposedEpoch");
    this.fAckConfig = new File(config.getLogDir(), "AckConfig");
    this.fProposedConfig = new File(config.getLogDir(), "ProposedConfig");

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
    this.transport = new NettyTransport(this.config.getServerId(), this);
    /*
    if (initialState.getTransportMap() == null) {
      this.transport = new DummyTransport(this.config.getServerId(), this);
    } else {
      this.transport = new DummyTransport(this.config.getServerId(),
                                          this,
                                          initialState.getTransportMap());
    }
    */
  }

  void send(ByteBuffer request) {
    if (this.currentState == State.LOOKING) {
      throw new RuntimeException("QuorumZab in LOOKING state can't serve "
          + "request.");
    } else if (this.currentState == State.LEADING) {
      Message msg = MessageBuilder.buildRequest(request);
      MessageTuple tuple = new MessageTuple(this.config.getServerId(), msg);
      this.messageQueue.add(tuple);
    } else {
      // It's FOLLOWING, sends to leader directly.
      LOG.debug("It's FOLLOWING state. Forwards the request to leader.");
      Message msg = MessageBuilder.buildRequest(request);
      // Forwards REQUEST to the leader.
      sendMessage(this.electedLeader, msg);
    }
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
    LOG.debug("ONDISCONNECTED from {}", serverId);
    Message disconnected = MessageBuilder.buildDisconnected(serverId);
    this.messageQueue.add(new MessageTuple(this.config.getServerId(),
                                          disconnected));
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
        this.isBroadcasting = false;
        this.stateMachine.stateChanged(State.LOOKING);
        this.currentState = State.LOOKING;
        MDC.put("state", "looking");
        MDC.put("phase", "electing");
        if (this.stateChangeCallback != null) {
          this.stateChangeCallback.electing();
        }

        startLeaderElection(electionAlg);
        waitForLeaderElected();

        LOG.debug("Selected {} as prospective leader.",
                  this.electedLeader);

        if (this.electedLeader.equals(this.config.getServerId())) {
          this.currentState = State.LEADING;
          MDC.put("state", "leading");
          lead();
        } else {
          this.currentState = State.FOLLOWING;
          MDC.put("state", "following");
          follow();
        }
      } catch (InterruptedException e) {
        LOG.debug("Caught Interrupted exception, it has been shut down?");
        this.transport.shutdown();
        return null;
      } catch (Exception e) {
        LOG.error("Caught exception :", e);
        return null;
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
   * Gets current configuration.
   *
   * @return the current configuration.
   * @throws IOException in case of IO failure.
   */
  Configuration getAckConfigFromFile() throws IOException {
    Properties prop = FileUtils.readPropertiesFromFile(this.fAckConfig);
    return Configuration.fromProperties(prop);
  }

  /**
   * Updates the current configuration.
   *
   * @param conf the updated current configuration.
   * @throws IOException in case of IO failure.
   */
  void setAckConfig(Configuration conf) throws IOException {
    FileUtils.writePropertiesToFile(conf.toProperties(), this.fAckConfig);
  }

  /**
   * Gets last proposed configuration.
   *
   * @return the last proposed configuration.
   * @throws IOException in case of IO failure.
   */
  Configuration getProposedConfigFromFile() throws IOException {
    Properties prop = FileUtils.readPropertiesFromFile(this.fProposedConfig);
    return Configuration.fromProperties(prop);
  }

  /**
   * Updates the last proposed configuration.
   *
   * @param conf the updated last proposed configuration.
   * @throws IOException in case of IO failure.
   */
  void setProposedConfig(Configuration conf) throws IOException {
    FileUtils.writePropertiesToFile(conf.toProperties(), this.fProposedConfig);
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
    } else if (tuple == MessageTuple.GO_BACK) {
      // Goes back to leader election.
      throw new BackToElectionException();
    } else if (tuple.getMessage().getType() == MessageType.PROPOSED_EPOCH &&
        this.currentState != State.LEADING) {
      // Explicitly close the connection when gets PROPOSED_EPOCH message in
      // FOLLOWING state to help the peer selecting the right leader faster.
      LOG.debug("Got PROPOSED_EPOCH in FOLLOWING state. Close the connection.");
      this.transport.clear(tuple.getSource());
    } else if (tuple.getMessage().getType() == MessageType.DISCONNECTED) {
      // Got DISCONNECTED message enqueued by onDisconnected callback.
      Message msg = tuple.getMessage();
      String serverId = msg.getDisconnected().getServerId();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got DISCONNECTED in getMessage().",
                  TextFormat.shortDebugString(msg));
      }
      if (this.currentState == State.LEADING) {
        // It's in LEADING state.
        if (this.quorumSet.containsKey(serverId)) {
          if (!this.isBroadcasting) {
            // If you lost someone who is in your quorumSet before broadcasting
            // phase, you are for sure not have a quorum of followers, just go
            // back to leader election. The clearance of the transport happens
            // in the exception handlers of lead function.
            LOG.debug("Lost follower {} in the quorumSet.", serverId);
            throw new BackToElectionException();
          } else {
            // Lost someone who is in the quorumSet in broadcasting phase,
            // return this message to caller and let it handles the
            // disconnection.
            LOG.debug("Lost follower {} in the quorumSet.", serverId);
          }
        } else {
          // Just lost someone you don't care, clear the transport so it can
          // join in in later time.
          LOG.debug("Lost follower {} outside quorumSet.", serverId);
          this.transport.clear(serverId);
        }
      } else {
        // FOLLOWING state.
        if (serverId.equals(this.electedLeader)) {
          // Disconnection from elected leader, going back to leader election,
          // the clearance of transport will happen in exception handlers of
          // follow function.
          LOG.debug("Lost elected leader {}.", this.electedLeader);
          throw new BackToElectionException();
        } else {
          // Lost connection to someone you don't care, clear transport.
          LOG.debug("Lost peer {}.", serverId);
          this.transport.clear(serverId);
        }
      }
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
    if (LOG.isTraceEnabled()) {
      LOG.trace("Sends to {} message {}",
                dest,
                TextFormat.shortDebugString(message));
    }
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

  void waitForLeaderElected() throws InterruptedException {
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
        if (LOG.isDebugEnabled()) {
          LOG.debug("Got unexpected message {} from {}.",
                    TextFormat.shortDebugString(msg),
                    source);
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
      SyncPeerTask syncTask = new SyncPeerTask(source, lastZxidPeer, lastZxid);
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
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got DIFF : {}",
                  TextFormat.shortDebugString(msg));
      }
      ZabMessage.Diff diff = msg.getDiff();
      // Remember last zxid of the peer.
      lastZxidPeer = MessageBuilder.fromProtoZxid(diff.getLastZxid());
      if(lastZxid.compareTo(lastZxidPeer) == 0) {
        // Means the two nodes have exact same history.
        return;
      }
    } else if (msg.getType() == MessageType.TRUNCATE) {
      // TRUNCATE message.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got TRUNCATE: {}",
                  TextFormat.shortDebugString(msg));
      }
      ZabMessage.Truncate trunc = msg.getTruncate();
      Zxid lastPrefixZxid = MessageBuilder
                            .fromProtoZxid(trunc.getLastPrefixZxid());
      this.log.truncate(lastPrefixZxid);
      return;
    } else {
      // SNAPSHOT message.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got SNAPSHOT: {}",
                 TextFormat.shortDebugString(msg));
      }
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

      if (LOG.isDebugEnabled()) {
        LOG.debug("Got PROPOSAL from {} : {}",
                  source,
                  TextFormat.shortDebugString(msg));
      }

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

  /**
   * Delivers all the transactions in the log after last delivered zxid.
   *
   * @throws IOException in case of IO failures.
   */
  void deliverUndeliveredTxns() throws IOException {
    Zxid startZxid = new Zxid(this.lastDeliveredZxid.getEpoch(),
                              this.lastDeliveredZxid.getXid() + 1);
    LOG.debug("Begins delivering all txns after {}", this.lastDeliveredZxid);
    try (Log.LogIterator iter = this.log.getIterator(startZxid)) {
      while (iter.hasNext()) {
        Transaction txn = iter.next();
        this.stateMachine.deliver(txn.getZxid(), txn.getBody(), null);
        this.lastDeliveredZxid = txn.getZxid();
      }
    }
  }


  /* -----------------   For LEADING state only -------------------- */

  /**
   * Begins executing leader steps. It returns if any exception is caught,
   * which causes it goes back to election phase.
   *
   * @throws InterruptedException in case of interrupted.
   */
  void lead() throws Exception {
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

      getPropsedEpochFromQuorum();
      proposeNewEpoch();
      waitEpochAckFromQuorum();

      LOG.debug("Established new epoch {}!",
                getProposedEpochFromFile());

      // Finds one who has the "best" history.
      String serverId = selectSyncHistoryOwner();
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
      beginSynchronizing();
      waitNewLeaderAckFromQuorum();

      // Broadcasts commit message.
      Message commit = MessageBuilder.buildCommit(this.log.getLatestZxid());
      broadcast(this.quorumSet.keySet().iterator(), commit);
      // Delivers all the txns in log before entering broadcasting phase.
      deliverUndeliveredTxns();

      /* -- Broadcasting phase -- */
      this.isBroadcasting = true;
      MDC.put("phase", "broadcast");
      LOG.debug("Now it's in broadcasting phase.");

      if (failCallback != null) {
        failCallback.leaderBroadcasting();
      }

      if (stateChangeCallback != null) {
        stateChangeCallback.leaderBroadcasting(getAckEpochFromFile(),
                                               getAllTxns());
      }
      for (PeerHandler ph : this.quorumSet.values()) {
        ph.startBroadcastingTask();
      }
      this.stateMachine.stateChanged(State.LEADING);
      beginBroadcasting();

    } catch (InterruptedException | TimeoutException | IOException |
        RuntimeException e) {
      // Shutdown all the PeerHandlers in quorum set.
      for (PeerHandler ph : this.quorumSet.values()) {
        ph.shutdown();
        this.quorumSet.remove(ph.getServerId());
      }
      if (e instanceof TimeoutException) {
        LOG.debug("Didn't hear message from peers for {} milliseconds. Going"
                  + " back to leader election.",
                  this.config.getTimeout());
      } else if (e instanceof InterruptedException) {
        LOG.debug("Participant is canceled by user.");
        throw (InterruptedException)e;
      } else if (e instanceof BackToElectionException) {
        LOG.debug("Got GO_BACK message from queue, going back to electing.");
      } else {
        LOG.error("Caught exception", e);
      }
    }
  }

  /**
   * Waits until receives the CEPOCH message from the quorum.
   *
   * @throws InterruptedException if anything wrong happens.
   * @throws TimeoutException in case of timeout.
   */
  void getPropsedEpochFromQuorum()
      throws InterruptedException, TimeoutException {
    // Gets last proposed epoch from other servers (not including leader).
    while (this.quorumSet.size() < getQuorumSize() - 1) {
      MessageTuple tuple = getExpectedMessage(MessageType.PROPOSED_EPOCH, null);
      Message msg = tuple.getMessage();
      String source = tuple.getSource();
      ProposedEpoch epoch = msg.getProposedEpoch();

      if (this.quorumSet.containsKey(source)) {
        throw new RuntimeException("Quorum set has already contained "
            + source + ", probably a bug?");
      }

      PeerHandler ph = new PeerHandler(source,
                                       this.transport,
                                       this.config.getTimeout() / 3);

      ph.setLastProposedEpoch(epoch.getProposedEpoch());
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
    epochs.add(getProposedEpochFromFile());

    for (PeerHandler stat : this.quorumSet.values()) {
      epochs.add(stat.getLastProposedEpoch());
    }

    int newEpoch = Collections.max(epochs) + 1;

    // Updates leader's last proposed epoch.
    setProposedEpoch(newEpoch);

    LOG.debug("Begins proposing new epoch {}",
              newEpoch);

    // Sends new epoch message to quorum.
    broadcast(this.quorumSet.keySet().iterator(),
              MessageBuilder.buildNewEpochMessage(newEpoch));
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
      String source = tuple.getSource();

      if (!this.quorumSet.containsKey(source)) {
        LOG.warn("The Epoch ACK comes from {} who is not in quorum set, "
                 + "possibly from previous epoch?",
                 source);
        continue;
      }

      ackCount++;

      AckEpoch ackEpoch = msg.getAckEpoch();
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

    // Get the acknowledged epoch and the latest zxid of the leader.
    int ackEpoch = getAckEpochFromFile();
    Zxid zxid = log.getLatestZxid();
    String serverId = config.getServerId();

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
   * @throws TimeoutException in case of timeout.
   * @throws InterruptedException in case of interrupt.
   */
  void waitNewLeaderAckFromQuorum()
      throws TimeoutException, InterruptedException, IOException {

    LOG.debug("Waiting for synchronization to followers complete.");

    int completeCount = 0;
    Zxid lastZxid = this.log.getLatestZxid();

    while (completeCount < this.quorumSet.size()) {
      MessageTuple tuple = getExpectedMessage(MessageType.ACK, null);
      ZabMessage.Ack ack = tuple.getMessage().getAck();
      String source =tuple.getSource();
      Zxid zxid = MessageBuilder.fromProtoZxid(ack.getZxid());

      if (zxid.compareTo(lastZxid) != 0) {
        LOG.error("The follower {} is not correctly synchronized.", source);
        throw new RuntimeException("The synchronized follower's last zxid"
            + "doesn't match last zxid of current leader.");
      }
      if (!this.quorumSet.containsKey(source)) {
        LOG.warn("Quorum set doesn't contain {}, a bug?", source);
        continue;
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
    Zxid lastZxid = this.log.getLatestZxid();
    int proposedEpoch = getProposedEpochFromFile();
    for (PeerHandler ph : this.quorumSet.values()) {
      ph.setSyncTask(new SyncPeerTask(ph.getServerId(),
                                      ph.getLastZxid(),
                                      lastZxid),
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
   * @throws ExecutionException
   */
  void beginBroadcasting()
      throws TimeoutException, InterruptedException, IOException {
    Zxid lastZxid = this.log.getLatestZxid();
    // Last committed zxid main thread has received.
    Zxid lastCommittedZxid = lastZxid;
    int currentEpoch = getAckEpochFromFile();
    // Add leader itself to quorumSet.
    PeerHandler lh = new PeerHandler(this.config.getServerId(),
                                     this.transport,
                                     this.config.getTimeout() / 3);
    lh.setLastAckedZxid(lastZxid);
    lh.startBroadcastingTask();
    this.quorumSet.put(this.config.getServerId(), lh);
    // Constructs all the processors.
    PreProcessor preProcessor= new PreProcessor(this.stateMachine,
                                                currentEpoch,
                                                this.quorumSet,
                                                this.config.getServerId());
    AckProcessor ackProcessor = new AckProcessor(this.quorumSet,
                                                 getQuorumSize(),
                                                 lastZxid);
    // Adds leader itself to quorum set.
    SyncProposalProcessor syncProcessor =
        new SyncProposalProcessor(this.log,
                                  this.transport,
                                  SYNC_MAX_BATCH_SIZE);
    CommitProcessor commitProcessor =
        new CommitProcessor(this.stateMachine,
                            this.lastDeliveredZxid);
    // The followers who joins in broadcasting phase and wait for their first
    // COMMIT message.
    Map<String, PeerHandler> pendingPeers = new HashMap<String, PeerHandler>();

    try {
      while (this.quorumSet.size() >= getQuorumSize()) {
        MessageTuple tuple = getMessage();
        Message msg = tuple.getMessage();
        String source = tuple.getSource();
        if (msg.getType() == MessageType.PROPOSED_EPOCH) {
          LOG.debug("Got PROPOSED_EPOCH from {}.", source);
          Message newEpoch = MessageBuilder.buildNewEpochMessage(currentEpoch);
          sendMessage(source, newEpoch);
        } else if (msg.getType() == MessageType.ACK_EPOCH) {
          LOG.debug("Got ACK_EPOCH from {}", source);
          onLeaderAckEpoch(source, msg, preProcessor);
        } else {
          // In broadcasting phase, the only expected messages come outside
          // the quorum set is PROPOSED_EPOCH and ACK_EPOCH.
          if (!this.quorumSet.containsKey(source)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got message {} from {} outside quorum.",
                        TextFormat.shortDebugString(msg),
                        source);
            }
            continue;
          }
          if (msg.getType() == MessageType.ACK) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got ACK {} from {}.",
                        TextFormat.shortDebugString(msg),
                        source);
            }
            onLeaderAck(source,
                        msg,
                        ackProcessor,
                        lastCommittedZxid,
                        pendingPeers);
          } else if (msg.getType() == MessageType.REQUEST) {
            LOG.debug("Got REQUEST from {}.", source);
            preProcessor.processRequest(new Request(source, msg));
          } else if (msg.getType() == MessageType.HEARTBEAT) {
            LOG.trace("Got HEARTBEAT replies from {}", source);
          } else if (msg.getType() == MessageType.FLUSH_PREPROCESSOR) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got FLUSH_PREPROCESSOR message {}",
                       TextFormat.shortDebugString(msg));
            }
            ZabMessage.FlushPreProcessor flush = msg.getFlushPreProcessor();
            Message flushSync = MessageBuilder
                                .buildFlushSyncProcessor(flush.getFollowerId());
            syncProcessor.processRequest(new Request(this.config.getServerId(),
                                                     flushSync));
          } else if (msg.getType() == MessageType.FLUSH_SYNCPROCESSOR) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got FLUSH_SYNCPROCESSOR message {}",
                       TextFormat.shortDebugString(msg));
            }
            onLeaderFlushSyncProcessor(source, msg, pendingPeers, ackProcessor);
          } else if (msg.getType() == MessageType.PROPOSAL) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got PROPOSAL message {}",
                       TextFormat.shortDebugString(msg));
            }
            Request req = new Request(source, msg);
            syncProcessor.processRequest(req);
            commitProcessor.processRequest(req);
          } else if (msg.getType() == MessageType.COMMIT) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got COMMIT message {}",
                       TextFormat.shortDebugString(msg));
            }
            lastCommittedZxid = MessageBuilder
                                .fromProtoZxid(msg.getCommit().getZxid());
            onLeaderCommit(source, msg, commitProcessor, pendingPeers);
          } else if (msg.getType() == MessageType.DISCONNECTED) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got DISCONNECTED message {}",
                        TextFormat.shortDebugString(msg));
            }
            onLeaderDisconnected(source,
                                 msg,
                                 pendingPeers,
                                 preProcessor,
                                 ackProcessor);
          } else {
            if (LOG.isWarnEnabled()) {
              LOG.warn("Unexpected messgae : {} from {}",
                       TextFormat.shortDebugString(msg),
                       source);
            }
          }
          // Updates last received message time for this follower.
          this.quorumSet.get(source).updateHeartbeatTime();
          // Checks if any peers in quorum set are dead.
          checkFollowerLiveness();
        }
      }
      LOG.debug("Detects the size of the ensemble is less than the"
          + "quorum size {}, goes back to electing phase.",
          getQuorumSize());
    } finally {
      // Stop all the processors.
      try {
        ackProcessor.shutdown();
        preProcessor.shutdown();
        commitProcessor.shutdown();
        syncProcessor.shutdown();
        // Updates last delivered zxid. Avoid delivering delivered transactions
        // next time even transactions are idempotent.
        this.lastDeliveredZxid = commitProcessor.getLastDeliveredZxid();
      } catch (ExecutionException e) {
        LOG.error("Caught exectuion exception.", e);
      }
    }
  }

  void onLeaderAckEpoch(String source,
                        Message msg,
                        PreProcessor preProcessor) {
    AckEpoch ackEpoch = msg.getAckEpoch();
    Zxid lastPeerZxid = MessageBuilder
                        .fromProtoZxid(ackEpoch.getLastZxid());
    PeerHandler ph = new PeerHandler(source,
                                     this.transport,
                                     this.config.getTimeout() / 3);
    ph.setLastZxid(lastPeerZxid);
    this.quorumSet.put(source, ph);
    Message flush = MessageBuilder.buildFlushPreProcessor(source);
    Message add = MessageBuilder.buildAddFollower(source);
    // Flush the pipeline before start synchronization.
    preProcessor.processRequest(new Request(null, flush));
    // Add new joined follower to PreProcessor.
    preProcessor.processRequest(new Request(null, add));
  }

  void onLeaderAck(String source,
                   Message msg,
                   AckProcessor ackProcessor,
                   Zxid lastCommittedZxid,
                   Map<String, PeerHandler> pendingPeers) {
    PeerHandler ph = pendingPeers.get(source);
    if (ph != null) {
      // Check if the sender of the ACK is in pending followers.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got ACK {} from pending peer {}.",
                  TextFormat.shortDebugString(msg),
                  source);
      }
      ZabMessage.Ack ack = msg.getAck();
      Zxid ackZxid = MessageBuilder.fromProtoZxid(ack.getZxid());
      ph.setLastAckedZxid(ackZxid);
      if (lastCommittedZxid.compareTo(ackZxid) >= 0) {
        // If the zxid of ACK is already committed, send COMMIT to follower and
        // start broadcasting task. Otherwise we need to wait until the zxid of
        // ACK is committed.
        LOG.debug("The ACK of pending peer is committed already, send COMMIT.");
        Message commit = MessageBuilder.buildCommit(ackZxid);
        sendMessage(ph.getServerId(), commit);
        ph.startBroadcastingTask();
        pendingPeers.remove(source);
      }
    }
    ackProcessor.processRequest(new Request(source, msg));
  }

  void onLeaderFlushSyncProcessor(String source,
                                  Message msg,
                                  Map<String, PeerHandler> pendingPeers,
                                  AckProcessor ackProcessor) throws
                                  IOException {
    // Starts synchronizing new joined follower once got FLUSH message.
    ZabMessage.FlushSyncProcessor flush = msg.getFlushSyncProcessor();
    String followerId = flush.getFollowerId();
    Message add = MessageBuilder.buildAddFollower(followerId);
    // Add new joined follower to AckProcessor.
    ackProcessor.processRequest(new Request(null, add));

    // Got last proposed zxid.
    Zxid lastSyncZxid = MessageBuilder
                        .fromProtoZxid(flush.getLastAppendedZxid());
    PeerHandler ph = this.quorumSet.get(followerId);
    ph.setSyncTask(new SyncPeerTask(followerId,
                                    ph.getLastZxid(),
                                    lastSyncZxid),
                   getAckEpochFromFile());
    ph.startSynchronizingTask();
    pendingPeers.put(followerId, ph);
  }

  void onLeaderCommit(String source,
                      Message msg,
                      CommitProcessor commitProcessor,
                      Map<String, PeerHandler> pendingPeers) {
    if (!pendingPeers.isEmpty()) {
      // If there're any new joined but uncommitted followers. Check if we can
      // send out first COMMIT message to them.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got COMMIT {} and there're pending peers.",
                  TextFormat.shortDebugString(msg));
      }
      Zxid zxidCommit = MessageBuilder.fromProtoZxid(msg.getCommit().getZxid());
      Iterator<Map.Entry<String, PeerHandler>> iter = pendingPeers.entrySet()
                                                                  .iterator();
      while (iter.hasNext()) {
        PeerHandler ph = iter.next().getValue();
        Zxid ackZxid = ph.getLastAckedZxid();
        if (ackZxid != null && zxidCommit.compareTo(ackZxid) >= 0) {
          LOG.debug("COMMIT >= last acked zxid of pending peer. Send COMMIT.");
          Message commit = MessageBuilder.buildCommit(ackZxid);
          sendMessage(ph.getServerId(), commit);
          ph.startBroadcastingTask();
          iter.remove();
        }
      }
    }
    commitProcessor.processRequest(new Request(source, msg));
  }

  void onLeaderDisconnected(String source,
                            Message msg,
                            Map<String, PeerHandler> pendingPeers,
                            PreProcessor preProcessor,
                            AckProcessor ackProcessor) {
    String followerId = msg.getDisconnected().getServerId();
    // Remove if it's in pendingPeers.
    pendingPeers.remove(followerId);
    PeerHandler ph = this.quorumSet.get(followerId);
    // Before calling shutdown, we need to disable PeerHandler first to prevent
    // sending obsolete messages in AckProcessor and preProcessor. Because once
    // we call shutdown(), the new connection from the peer is allowed, and
    // AckProcessor and PreProcessor should not send obsolete messages in new
    // connection.
    ph.disableSending();
    // Stop PeerHandler thread and clear tranport.
    ph.shutdown();
    this.quorumSet.remove(followerId);
    Message remove = MessageBuilder.buildRemoveFollower(followerId);
    Request req = new Request(null, remove);
    // Ask PreProcessor to remove this follower.
    preProcessor.processRequest(req);
    // Ask AckProcessor to remove this follower.
    ackProcessor.processRequest(req);
  }

  void checkFollowerLiveness() {
    long currentTime = System.nanoTime();
    long timeoutNs = this.config.getTimeout() * (long)1000000;
    for (PeerHandler ph : this.quorumSet.values()) {
      if (ph.getServerId().equals(this.config.getServerId())) {
        continue;
      }
      if (currentTime - ph.getLastHeartbeatTime() >= timeoutNs) {
        // Removes the peer who is likely to be dead.
        String serverId = ph.getServerId();
        LOG.warn("{} is likely to be dead, enqueue a DISCONNECTED message.",
                 serverId);
        // Enqueue a DISCONNECTED message.
        Message disconnected = MessageBuilder.buildDisconnected(serverId);
        this.messageQueue.add(new MessageTuple(this.config.getServerId(),
                                               disconnected));
      }
    }
  }

  /* -----------------   For FOLLOWING state only -------------------- */

  /**
   * Begins executing follower steps. It returns if any exception is caught,
   * which causes it goes back to election phase.
   * @throws InterruptedException
   */
  void follow() throws InterruptedException {

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
      waitForNewLeaderMesage();
      waitForCommitMessage();
      // Delivers all transactions in log before entering broadcasting phase.
      deliverUndeliveredTxns();

      /* -- Broadcasting phase -- */
      this.isBroadcasting = true;
      MDC.put("phase", "broadcast");
      LOG.debug("Now it's in broadcasting phase.");

      if (stateChangeCallback != null) {
        stateChangeCallback.followerBroadcasting(getAckEpochFromFile(),
                                                 getAllTxns());
      }

      if (failCallback != null) {
        failCallback.followerBroadcasting();
      }
      this.stateMachine.stateChanged(State.FOLLOWING);
      beginAccepting();

    } catch (InterruptedException | TimeoutException | IOException |
        RuntimeException e) {
      if (e instanceof TimeoutException) {
        LOG.debug("Didn't hear message from {} for {} milliseconds. Going"
                  + " back to leader election.",
                  this.electedLeader,
                  this.config.getTimeout());
      } else if (e instanceof InterruptedException) {
        LOG.debug("Participant is canceled by user.");
        throw (InterruptedException)e;
      } else if (e instanceof BackToElectionException) {
        LOG.debug("Got GO_BACK message from queue, going back to electing.");
      } else {
        LOG.error("Caught exception", e);
      }
    } finally {
      this.transport.clear(this.electedLeader);
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
  void waitForNewLeaderMesage()
      throws TimeoutException, InterruptedException, IOException {
    LOG.debug("Waiting for New Leader message from {}.", this.electedLeader);
    MessageTuple tuple = getExpectedMessage(MessageType.NEW_LEADER,
                                            this.electedLeader);
    Message msg = tuple.getMessage();
    String source = tuple.getSource();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Got NEW_LEADER message from {} : {}.",
                source,
                TextFormat.shortDebugString(msg));
    }

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
   * Wait for a commit message from the leader.
   *
   * @throws TimeoutException in case of timeout.
   * @throws InterruptedException in case of interruption.
   * @throws IOException in case of IO failures.
   */
  void waitForCommitMessage()
      throws TimeoutException, InterruptedException, IOException {
    LOG.debug("Waiting for commit message from {}", this.electedLeader);
    MessageTuple tuple = getExpectedMessage(MessageType.COMMIT,
                                            this.electedLeader);
    Zxid zxid = MessageBuilder.fromProtoZxid(tuple.getMessage()
                                                  .getCommit()
                                                  .getZxid());
    Zxid lastZxid = this.log.getLatestZxid();
    // If the followers are appropriately synchronized, the Zxid of ACK should
    // match the last Zxid in followers' log.
    if (zxid.compareTo(lastZxid) != 0) {
      LOG.error("The ACK zxid {} doesn't match last zxid {} in log!",
                zxid,
                lastZxid);
      throw new RuntimeException("The ACK zxid doesn't match last zxid");
    }
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
        new SyncProposalProcessor(this.log, this.transport,
                                  SYNC_MAX_BATCH_SIZE);

    CommitProcessor commitProcessor
      = new CommitProcessor(stateMachine, this.lastDeliveredZxid);

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
         *  3) HEARTBEAT : Leader will send HEARTBEAT messages to followers
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
            // Dispatch to SyncProposalProcessor and CommitProcessor.
            Request req = new Request(this.electedLeader, msg);
            syncProcessor.processRequest(req);
            commitProcessor.processRequest(req);
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
        } else if (msg.getType() == MessageType.HEARTBEAT) {
          LOG.trace("Got HEARTBEAT from {}.",
                    source);
          // Replies HEARTBEAT message to leader.
          Message heartbeatReply = MessageBuilder.buildHeartbeat();
          sendMessage(source, heartbeatReply);
        } else if (msg.getType() == MessageType.DISCONNECTED) {
          LOG.debug("Ignores DISCONNECTED message in FOLLOWING state.");
        } else {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Unexpected messgae : {} from {}",
                      TextFormat.shortDebugString(msg),
                     source);
          }
        }
      }
    } finally {
      try {
        commitProcessor.shutdown();
        syncProcessor.shutdown();
        // Updates last delivered zxid. Avoid delivering delivered transactions
        // next time even transactions are idempotent.
        this.lastDeliveredZxid = commitProcessor.getLastDeliveredZxid();
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
    private final Zxid lastSyncZxid;

   /**
    * Constructs the SyncPeerTask object.
    *
    * @param peerId the id of the peer.
    * @param peerLatestZxid the last zxid of the peer.
    * @param lastSyncZxid leader will synchronize the follower up to this zxid.
    */
    public SyncPeerTask(String peerId, Zxid peerLatestZxid, Zxid lastSyncZxid) {
      this.peerId = peerId;
      this.peerLatestZxid = peerLatestZxid;
      this.lastSyncZxid = lastSyncZxid;
    }

    public Zxid getLastSyncZxid() {
      return this.lastSyncZxid;
    }

    public void run() throws IOException {
      LOG.debug("Begins synchronizing history to {}(last zxid : {})",
                peerId,
                peerLatestZxid);
      Zxid syncPoint = null;

      if (this.lastSyncZxid.getEpoch() == peerLatestZxid.getEpoch()) {
        // If the peer has same epoch number as the server.
        if (this.lastSyncZxid.compareTo(peerLatestZxid) >= 0) {
          // Means peer's history is the prefix of the server's.
          LOG.debug("{}'s history is >= {}'s, sending DIFF.",
                    config.getServerId(),
                    peerId);
          syncPoint = new Zxid(peerLatestZxid.getEpoch(),
                               peerLatestZxid.getXid() + 1);
          Message diff = MessageBuilder.buildDiff(this.lastSyncZxid);
          sendMessage(peerId, diff);
        } else {
          // Means peer's history is the superset of the server's.
          LOG.debug("{}'s history is < {}'s, sending TRUNCATE.",
                    config.getServerId(),
                    peerId);
          // Doesn't need to synchronize anything, just truncate.
          syncPoint = null;
          Message trunc = MessageBuilder.buildTruncate(this.lastSyncZxid);
          sendMessage(peerId, trunc);
        }
      } else {
        // They have different epoch numbers. Truncate all.
        LOG.debug("The last epoch of {} and {} are different, sending "
                  + "SNAPSHOT.",
                  config.getServerId(),
                  peerId);
        syncPoint = new Zxid(0, 0);
        Message snapshot = MessageBuilder.buildSnapshot(this.lastSyncZxid);
        sendMessage(peerId, snapshot);
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
    }
  }
}
