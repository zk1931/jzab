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
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.zab.proto.ZabMessage;
import org.apache.zab.proto.ZabMessage.AckEpoch;
import org.apache.zab.proto.ZabMessage.Message;
import org.apache.zab.proto.ZabMessage.NewEpoch;
import org.apache.zab.proto.ZabMessage.ProposedEpoch;
import org.apache.zab.proto.ZabMessage.Message.MessageType;
import org.apache.zab.transport.NettyTransport;
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
   * The file to store the last acknowledged config.
   */
  private final File fLastSeenConfig;

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

  /**
   * The ID/Address of the participant.
   */
  private final String serverId;

  /**
   * The cached last seen configuration.
   */
  ClusterConfiguration clusterConfiguration = null;

  private static final Logger LOG = LoggerFactory.getLogger(Participant.class);

  /**
   * The state of Zab.
   */
  public enum State {
    LOOKING,
    LEADING,
    FOLLOWING
  }

  /**
   * The phase of Zab.
   */
  public enum Phase {
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
  static class Exit extends RuntimeException {
    public Exit(String desc) {
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

  public Participant(ZabConfig config,
                     StateMachine stateMachine)
      throws IOException, InterruptedException {
    this(stateMachine, null, null, config, null);
  }

  Participant(StateMachine stateMachine,
              StateChangeCallback cb,
              FailureCaseCallback fcb,
              ZabConfig config,
              Log log) throws
              IOException, InterruptedException {
    this.config = config;
    this.stateMachine = stateMachine;
    File logDir = new File(config.getLogDir());
    LOG.debug("Trying to create log directory {}", logDir.getAbsolutePath());
    if (!logDir.mkdir()) {
      LOG.debug("Creating log directory {} failed, already exists?",
                logDir.getAbsolutePath());
    }
    this.fAckEpoch = new File(config.getLogDir(), "ack_epoch");
    this.fProposedEpoch = new File(config.getLogDir(), "proposed_epoch");
    this.fLastSeenConfig = new File(config.getLogDir(), "cluster_config");
    if (log == null) {
      File logFile = new File(this.config.getLogDir(), "transaction.log");
      this.log = new SimpleLog(logFile);
    } else {
      this.log = log;
    }
    if (this.config.getJoinPeer() != null) {
      LOG.debug("Joining peer {}", this.config.getJoinPeer());
      // User should clean all old data before joining.
      if (this.fAckEpoch.exists() || this.fProposedEpoch.exists() ||
          this.fLastSeenConfig.exists()) {
        LOG.error("The log directory is not empty.");
        throw new RuntimeException("Log directory must be empty before join.");
      }
      this.log.truncate(Zxid.ZXID_NOT_EXIST);
      this.serverId = this.config.getServerId();
    } else {
      // Means either it starts booting from static configuration or recovering
      // from a log directory.
      if (this.config.getPeers().size() > 0) {
        // TODO : Static configuration should be removed eventually.
        LOG.debug("Boots from static configuration.");
        // User has specified server list.
        List<String> peers = this.config.getPeers();
        this.serverId = this.config.getServerId();
        ClusterConfiguration cnf = new ClusterConfiguration(Zxid.ZXID_NOT_EXIST,
                                                            peers,
                                                            this.serverId);
        setLastSeenConfig(cnf);
      } else {
        // Restore from log directory.
        LOG.debug("Restores from log directory {}", this.config.getLogDir());
        ClusterConfiguration cnf = getLastSeenConfig();
        if (cnf == null) {
          throw new RuntimeException("Can't find configuration file.");
        }
        this.serverId = cnf.getServerId();
      }
    }
    MDC.put("serverId", this.serverId);
    this.stateChangeCallback = cb;
    this.failCallback = fcb;
    this.transport = new NettyTransport(this.serverId, this);
  }

  void send(ByteBuffer request) {
    if (this.currentState == State.LOOKING) {
      throw new RuntimeException("QuorumZab in LOOKING state can't serve "
          + "request.");
    } else if (this.currentState == State.LEADING) {
      Message msg = MessageBuilder.buildRequest(request);
      MessageTuple tuple = new MessageTuple(this.serverId, msg);
      this.messageQueue.add(tuple);
    } else {
      // It's FOLLOWING, sends to leader directly.
      LOG.debug("It's FOLLOWING state. Forwards the request to leader.");
      Message msg = MessageBuilder.buildRequest(request);
      // Forwards REQUEST to the leader.
      sendMessage(this.electedLeader, msg);
    }
  }

  public void leave() {
    if (!this.isBroadcasting) {
      LOG.warn("Can leave the cluster only if you are in broaccsting phase.");
      throw new RuntimeException("Leave cluster in revoering phase.");
    }
    Message msg = MessageBuilder.buildLeave(this.serverId);
    if (this.currentState == State.LEADING) {
      MessageTuple tuple = new MessageTuple(this.serverId, msg);
      this.messageQueue.add(tuple);
    } else {
      sendMessage(this.electedLeader, msg);
    }
  }

  String getServerId() {
    return this.serverId;
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
  public void onDisconnected(String peerId) {
    LOG.debug("ONDISCONNECTED from {}", peerId);
    Message disconnected = MessageBuilder.buildDisconnected(peerId);
    this.messageQueue.add(new MessageTuple(this.serverId,
                                           disconnected));
  }

  /**
   * Change the state of current participant.
   *
   * @param state the state change(LOOKING/FOLLOWING/LEADING).
   */
  void changeState(State state) {
    if (state == State.LOOKING) {
      MDC.put("state", "looking");
      if (this.stateChangeCallback != null) {
        this.stateChangeCallback.electing();
      }
    } else if (state == State.FOLLOWING) {
      MDC.put("state", "following");
    } else if (state == State.LEADING) {
      MDC.put("state", "leading");
    }
    this.currentState = state;
  }

  /**
   * Change the phase of current participant.
   *
   * @param phase the phase change(DISCOVERING/SYNCHRONIZING/BROADCASTING).
   * @throws IOException in case of IO failure.
   */
  void changePhase(Phase phase) throws IOException {
    if (phase == Phase.DISCOVERING) {
      MDC.put("phase", "discovering");
      if (this.currentState == State.LEADING) {
        if (stateChangeCallback != null) {
          stateChangeCallback.leaderDiscovering(this.serverId);
        }
        if (failCallback != null) {
          failCallback.leaderDiscovering();
        }
      } else if (this.currentState == State.FOLLOWING) {
        if (stateChangeCallback != null) {
          stateChangeCallback.followerDiscovering(this.electedLeader);
        }
        if (failCallback != null) {
          failCallback.followerDiscovering();
        }
      }
    } else if (phase == Phase.SYNCHRONIZING) {
      MDC.put("phase", "synchronizing");
      if (this.currentState == State.LEADING) {
        if (stateChangeCallback != null) {
          stateChangeCallback.leaderSynchronizing(getProposedEpochFromFile());
        }
        if (failCallback != null) {
          failCallback.leaderSynchronizing();
        }
      } else if (this.currentState == State.FOLLOWING) {
        if (stateChangeCallback != null) {
          stateChangeCallback.followerSynchronizing(getProposedEpochFromFile());
        }
        if (failCallback != null) {
          failCallback.followerSynchronizing();
        }
      }
    } else if (phase == Phase.BROADCASTING) {
      MDC.put("phase", "broadcasting");
      this.isBroadcasting = true;
      if (this.currentState == State.LEADING) {
        if (failCallback != null) {
          failCallback.leaderBroadcasting();
        }
        if (stateChangeCallback != null) {
          stateChangeCallback.leaderBroadcasting(getAckEpochFromFile(),
                                                 getAllTxns(),
                                                 getLastSeenConfig());
        }
      } else if (this.currentState == State.FOLLOWING) {
        if (stateChangeCallback != null) {
          stateChangeCallback.followerBroadcasting(getAckEpochFromFile(),
                                                   getAllTxns(),
                                                   getLastSeenConfig());
        }
        if (failCallback != null) {
          failCallback.followerBroadcasting();
        }
      }
    }
  }

  /**
   * Starts main logic of participant.
   */
  @Override
  public Void call() throws Exception {
    LOG.debug("Participant starts running.");
    Election electionAlg = new RoundRobinElection();
    try {
      if (this.config.getJoinPeer() != null) {
        this.stateMachine.recovering();
        join(this.config.getJoinPeer());
      }
      while (true) {
        this.isBroadcasting = false;
        this.stateMachine.recovering();
        this.currentState = State.LOOKING;
        changeState(State.LOOKING);
        startLeaderElection(electionAlg);
        waitForLeaderElected();
        LOG.debug("Selected {} as prospective leader.",
                  this.electedLeader);
        if (this.electedLeader.equals(this.serverId)) {
          changeState(State.LEADING);
          lead();
        } else {
          changeState(State.FOLLOWING);
          follow();
        }
      }
    } catch (InterruptedException e) {
      LOG.debug("Caught Interrupted exception, it has been shut down?");
      this.transport.shutdown();
    } catch (Exit e) {
      LOG.debug("Exit Participant");
    } catch (Exception e) {
      LOG.error("Caught exception :", e);
      throw e;
    }
    if (this.stateChangeCallback != null) {
      this.stateChangeCallback.exit();
    }
    return null;
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
   * Gets last seen configuration.
   *
   * @return the last seen configuration.
   * @throws IOException in case of IO failure.
   */
  ClusterConfiguration getLastSeenConfig() throws IOException {
    if (this.clusterConfiguration != null) {
      // Returns cached cluster configuration instead of reading from file.
      // Caching here has significant impacts on the throughput of the system.
      return this.clusterConfiguration;
    }
    try {
      Properties prop = FileUtils.readPropertiesFromFile(this.fLastSeenConfig);
      return ClusterConfiguration.fromProperties(prop);
    } catch (FileNotFoundException e) {
      LOG.debug("AckConfig file doesn't exist, probably it's the first time" +
          "bootup.");
    }
    return null;
  }

  /**
   * Updates the last seen configuration.
   *
   * @param conf the updated configuration.
   * @throws IOException in case of IO failure.
   */
  void setLastSeenConfig(ClusterConfiguration conf) throws IOException {
    this.clusterConfiguration = conf;
    this.stateMachine.clusterChange(new HashSet<String>(conf.getPeers()));
    FileUtils.writePropertiesToFile(conf.toProperties(), this.fLastSeenConfig);
  }

  /**
   * Gets a message from the queue.
   *
   * @return a message tuple contains the message and its source.
   * @throws TimeoutException in case of timeout.
   * @throws InterruptedException it's interrupted.
   */
  MessageTuple getMessage() throws TimeoutException, InterruptedException {
    while (true) {
      MessageTuple tuple = messageQueue.poll(config.getTimeout(),
                                             TimeUnit.MILLISECONDS);
      if (tuple == null) {
        // Timeout.
        throw new TimeoutException("Timeout while waiting for the message.");
      } else if (tuple == MessageTuple.GO_BACK) {
        // Goes back to leader election.
        throw new BackToElectionException();
      } else if (tuple.getMessage().getType() == MessageType.PROPOSED_EPOCH &&
                 this.currentState != State.LEADING) {
        // Explicitly close the connection when gets PROPOSED_EPOCH message in
        // FOLLOWING state to help the peer selecting the right leader faster.
        LOG.debug("Got PROPOSED_EPOCH in FOLLOWING state. Close connection.");
        this.transport.clear(tuple.getServerId());
      } else if (tuple.getMessage().getType() == MessageType.DISCONNECTED) {
        // Got DISCONNECTED message enqueued by onDisconnected callback.
        Message msg = tuple.getMessage();
        String peerId = msg.getDisconnected().getServerId();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Got DISCONNECTED in getMessage().",
                    TextFormat.shortDebugString(msg));
        }
        if (this.currentState == State.LEADING) {
          // It's in LEADING state.
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
          // FOLLOWING state.
          if (this.electedLeader != null && peerId.equals(this.electedLeader)) {
            // Disconnection from elected leader, going back to leader election,
            // the clearance of transport will happen in exception handlers of
            // follow/join function.
            LOG.debug("Lost elected leader {}.", this.electedLeader);
            throw new BackToElectionException();
          } else {
            // Lost connection to someone you don't care, clear transport.
            LOG.debug("Lost peer {}.", peerId);
            this.transport.clear(peerId);
          }
        }
      } else {
        return tuple;
      }
    }
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
   *
   * @throws IOException in case of IO failures.
   */
  @Override
  public int getEnsembleSize() throws IOException {
    return getServerList().size();
  }

  /**
   * Gets the server list.
   *
   * @throws IOException in case of IO failures.
   */
  @Override
  public List<String> getServerList() throws IOException {
    ClusterConfiguration cnf = getLastSeenConfig();
    return cnf.getPeers();
  }

  /**
   * Gets the minimal quorum size.
   *
   * @return the minimal quorum size
   * @throws IOException in case of IO failures.
   */
  @Override
  public int getQuorumSize() throws IOException {
    return getEnsembleSize() / 2 + 1;
  }

  /**
   * Broadcasts the message to all the peers.
   *
   * @param peers the destination of peers.
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
      LOG.trace("Sends message {} to {}.",
                TextFormat.shortDebugString(message),
                dest);
    }
    this.transport.send(dest, ByteBuffer.wrap(message.toByteArray()));
  }


  /* -----------------   For ELECTING state only -------------------- */
  void startLeaderElection(Election electionAlg) throws Exception {
    this.leaderCondition = new CountDownLatch(1);
    electionAlg.initialize(this, this);
  }

  @Override
  public void leaderElected(String peerId) {
    this.electedLeader = peerId;
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
      SyncPeerTask syncTask = new SyncPeerTask(source, lastZxidPeer, lastZxid,
                                               getLastSeenConfig());
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
        LOG.debug("Got DIFF : {}",
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
      // TRUNCATE message. If the peer's history is the prefix of this
      // Participant, just trucate this Participant's history up to the
      // last prefix zxid.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got TRUNCATE: {}",
                  TextFormat.shortDebugString(msg));
      }
      ZabMessage.Truncate trunc = msg.getTruncate();
      Zxid lastPrefixZxid = MessageBuilder
                            .fromProtoZxid(trunc.getLastPrefixZxid());
      this.log.truncate(lastPrefixZxid);
      waitForSyncEnd(peer);
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
        LOG.debug("Got PROPOSAL from {} : {}", source,
                  TextFormat.shortDebugString(msg));
      }
      ZabMessage.Proposal prop = msg.getProposal();
      Zxid zxid = MessageBuilder.fromProtoZxid(prop.getZxid());
      // Accept the proposal.
      this.log.append(MessageBuilder.fromProposal(prop));
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
    setLastSeenConfig(cnf);
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
   */
  void lead() throws Exception {
    try {
      /* -- Discovering phase -- */
      changePhase(Phase.DISCOVERING);
      waitProposedEpochFromQuorum();
      proposeNewEpoch();
      waitEpochAckFromQuorum();
      LOG.debug("Established new epoch {}",
                getProposedEpochFromFile());
      // Finds one who has the "best" history.
      String peerId = selectSyncHistoryOwner();
      LOG.debug("Chooses {} to pull its history.", peerId);

      /* -- Synchronizing phase -- */
      changePhase(Phase.SYNCHRONIZING);
      if (!peerId.equals(this.serverId)) {
        // Pulls history from the follower.
        synchronizeFromFollower(peerId);
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
      changePhase(Phase.BROADCASTING);
      for (PeerHandler ph : this.quorumSet.values()) {
        ph.startBroadcastingTask();
      }
      beginBroadcasting();
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
    } catch (Exit e) {
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
   * Waits until receives the CEPOCH message from the quorum.
   *
   * @throws InterruptedException if anything wrong happens.
   * @throws TimeoutException in case of timeout.
   */
  void waitProposedEpochFromQuorum()
      throws InterruptedException, TimeoutException, IOException {
    ClusterConfiguration currentConfig = getLastSeenConfig();
    int acknowledgedEpoch = getAckEpochFromFile();

    while (this.quorumSet.size() < getQuorumSize() - 1) {
      MessageTuple tuple = getExpectedMessage(MessageType.PROPOSED_EPOCH, null);
      Message msg = tuple.getMessage();
      String source = tuple.getServerId();
      ProposedEpoch epoch = msg.getProposedEpoch();
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
      PeerHandler ph = new PeerHandler(source,
                                       this.transport,
                                       this.config.getTimeout() / 3);
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
    epochs.add(getProposedEpochFromFile());
    for (PeerHandler stat : this.quorumSet.values()) {
      epochs.add(stat.getLastProposedEpoch());
    }
    int newEpoch = Collections.max(epochs) + 1;
    // Updates leader's last proposed epoch.
    setProposedEpoch(newEpoch);
    LOG.debug("Begins proposing new epoch {}", newEpoch);
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
      String source = tuple.getServerId();
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
    int ackEpoch = getAckEpochFromFile();
    Zxid zxid = log.getLatestZxid();
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
    Zxid lastZxid = log.getLatestZxid();
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
    Zxid lastZxid = this.log.getLatestZxid();
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
    Zxid lastZxid = this.log.getLatestZxid();
    ClusterConfiguration clusterConfig = getLastSeenConfig();
    int proposedEpoch = getProposedEpochFromFile();
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
  void beginBroadcasting()
      throws TimeoutException, InterruptedException, IOException,
      ExecutionException {
    Zxid lastZxid = this.log.getLatestZxid();
    // Last committed zxid main thread has received.
    Zxid lastCommittedZxid = lastZxid;
    int currentEpoch = getAckEpochFromFile();
    // Add leader itself to quorumSet.
    PeerHandler lh = new PeerHandler(this.serverId, this.transport,
                                     this.config.getTimeout() / 3);
    lh.setLastAckedZxid(lastZxid);
    lh.startBroadcastingTask();
    this.quorumSet.put(this.serverId, lh);
    PreProcessor preProcessor= new PreProcessor(this.stateMachine,
                                                currentEpoch,
                                                this.quorumSet,
                                                this.serverId);
    AckProcessor ackProcessor = new AckProcessor(this.quorumSet,
                                                 this,
                                                 lastZxid);
    SyncProposalProcessor syncProcessor =
        new SyncProposalProcessor(this.log, this.transport,
                                  SYNC_MAX_BATCH_SIZE);
    CommitProcessor commitProcessor =
        new CommitProcessor(this.stateMachine, this.lastDeliveredZxid);
    // The followers who join in broadcasting phase and wait for their first
    // COMMIT message.
    Map<String, PeerHandler> pendingPeers = new HashMap<String, PeerHandler>();
    // The reconfiguration in progress. It's no if there's no pending reconfig.
    ClusterConfiguration pendingConfig = null;
    // The COP acknowledgements for current pending configuration.
    int ackCopCount = 0;
    this.stateMachine.leading(this.quorumSet.keySet());
    try {
      while (this.quorumSet.size() >= getQuorumSize()) {
        MessageTuple tuple = getMessage();
        Message msg = tuple.getMessage();
        String source = tuple.getServerId();
        if (msg.getType() == MessageType.PROPOSED_EPOCH) {
          LOG.debug("Got PROPOSED_EPOCH from {}.", source);
          ClusterConfiguration cnf = getLastSeenConfig();
          if (!cnf.contains(source)) {
            // Only allows servers who are in the current config to join.
            LOG.warn("Got PROPOSED_EPOCH from {} who is not in config, "
                + "ignores it.", source);
            continue;
          }
          Message newEpoch = MessageBuilder.buildNewEpochMessage(currentEpoch);
          sendMessage(source, newEpoch);
        } else if (msg.getType() == MessageType.ACK_EPOCH) {
          LOG.debug("Got ACK_EPOCH from {}", source);
          onLeaderAckEpoch(source, msg, preProcessor);
        } else if (msg.getType() == MessageType.QUERY_LEADER) {
          LOG.debug("Got QUERY_LEADER from {}", source);
          Message reply = MessageBuilder.buildQueryReply(this.serverId);
          sendMessage(source, reply);
        } else if (msg.getType() == MessageType.JOIN) {
          LOG.debug("Got JOIN from {}", source);
          if (pendingConfig != null) {
            LOG.warn("There's a pending configuration, ignore JOIN request.");
            continue;
          }
          pendingConfig = onLeaderJoin(source, preProcessor, currentEpoch);
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
          if (msg.getType() == MessageType.ACK) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got ACK {} from {}.",
                        TextFormat.shortDebugString(msg),
                        source);
            }
            onLeaderAck(source, msg, ackProcessor, lastCommittedZxid,
                        pendingPeers);
          } else if (msg.getType() == MessageType.REQUEST) {
            LOG.debug("Got REQUEST from {}.", source);
            preProcessor.processRequest(tuple);
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
            syncProcessor.processRequest(new MessageTuple(this.serverId,
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
            syncProcessor.processRequest(tuple);
            commitProcessor.processRequest(tuple);
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
            onLeaderDisconnected(source, msg, pendingPeers, preProcessor,
                                 ackProcessor);
          } else if (msg.getType() == MessageType.COP) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got message COP {}", TextFormat.shortDebugString(msg));
            }
            ClusterConfiguration cnf =
              ClusterConfiguration.fromProto(msg.getConfig(), this.serverId);
            setLastSeenConfig(cnf);
            Message ackCop = MessageBuilder.buildAckCop(cnf.getVersion());
            sendMessage(this.serverId, ackCop);
          } else if (msg.getType() == MessageType.LEAVE) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got message LEAVE {}",
                        TextFormat.shortDebugString(msg));
            }
            if (pendingConfig != null) {
              LOG.warn("There's a pending configuration, ignore LEAVE request");
              continue;
            }
            pendingConfig = onLeaderLeave(source, msg, pendingPeers,
                                          preProcessor, ackProcessor);
          } else if (msg.getType() == MessageType.ACK_COP) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got message ACK_COP {} from {}",
                        TextFormat.shortDebugString(msg), source);
            }
            Zxid version =
              MessageBuilder.fromProtoZxid(msg.getAckCop().getVersion());
            if (pendingConfig != null &&
                pendingConfig.getVersion().equals(version) &&
                pendingConfig.contains(source)) {
              ackCopCount++;
              // Checks if we got ACK from a quorum of machines.
              if (ackCopCount == pendingConfig.getPeers().size() / 2 + 1) {
                LOG.debug("ACK_COP for {} is committed from a quorum size {}",
                          version, ackCopCount);
                if (this.stateChangeCallback != null) {
                  this.stateChangeCallback.commitCop();
                }
                if (!pendingConfig.contains(this.serverId)) {
                  LOG.debug("Leader is not in new configuration, exiting.");
                  throw new Exit("Exit because it's not in new configuration.");
                }
                ackCopCount = 0;
                pendingConfig = null;
              }
            }
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
      ackProcessor.shutdown();
      preProcessor.shutdown();
      commitProcessor.shutdown();
      syncProcessor.shutdown();
      this.lastDeliveredZxid = commitProcessor.getLastDeliveredZxid();
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
    this.stateMachine.leading(this.quorumSet.keySet());
    Message flush = MessageBuilder.buildFlushPreProcessor(source);
    Message add = MessageBuilder.buildAddFollower(source);
    // Flush the pipeline before start synchronization.
    preProcessor.processRequest(new MessageTuple(null, flush));
    // Add new joined follower to PreProcessor.
    preProcessor.processRequest(new MessageTuple(null, add));
  }

  ClusterConfiguration onLeaderJoin(String source,
                                    PreProcessor preProcessor,
                                    int currentEpoch) throws IOException {
    PeerHandler ph = new PeerHandler(source,
                                     this.transport,
                                     this.config.getTimeout() / 3);
    ph.setLastZxid(Zxid.ZXID_NOT_EXIST);
    this.quorumSet.put(source, ph);
    this.stateMachine.leading(this.quorumSet.keySet());
    Message flush = MessageBuilder.buildFlushPreProcessor(source);
    Message add = MessageBuilder.buildAddFollower(source);
    ClusterConfiguration clusterConfig = getLastSeenConfig();
    Zxid version = clusterConfig.getVersion();
    if (version.getEpoch() != currentEpoch) {
      clusterConfig.setVersion(new Zxid(currentEpoch, 0));
    } else {
      clusterConfig.setVersion(new Zxid(currentEpoch, version.getXid() + 1));
    }
    if (!clusterConfig.contains(source)) {
      // We allow peer rejoins the cluster. So probably the new joined
      // peer has been already in current configuration file.
      clusterConfig.addPeer(source);
    }
    Message cop = MessageBuilder.buildCop(clusterConfig);
    // Flush the pipeline before start synchronization.
    preProcessor.processRequest(new MessageTuple(null, flush));
    // Add new joined follower to PreProcessor.
    preProcessor.processRequest(new MessageTuple(null, add));
    // Broadcast COP message to all peers.
    preProcessor.processRequest(new MessageTuple(null, cop));
    return clusterConfig;
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
    ackProcessor.processRequest(new MessageTuple(source, msg));
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
    ackProcessor.processRequest(new MessageTuple(null, add));
    // Got last proposed zxid.
    Zxid lastSyncZxid = MessageBuilder
                        .fromProtoZxid(flush.getLastAppendedZxid());
    PeerHandler ph = this.quorumSet.get(followerId);
    ph.setSyncTask(new SyncPeerTask(followerId,
                                    ph.getLastZxid(),
                                    lastSyncZxid,
                                    getLastSeenConfig()),
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
    commitProcessor.processRequest(new MessageTuple(source, msg));
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
    this.stateMachine.leading(this.quorumSet.keySet());
    Message remove = MessageBuilder.buildRemoveFollower(followerId);
    MessageTuple req = new MessageTuple(null, remove);
    // Ask PreProcessor to remove this follower.
    preProcessor.processRequest(req);
    // Ask AckProcessor to remove this follower.
    ackProcessor.processRequest(req);
  }

  ClusterConfiguration onLeaderLeave(String source,
                                     Message msg,
                                     Map<String, PeerHandler> pendingPeers,
                                     PreProcessor preProcessor,
                                     AckProcessor ackProcessor)
      throws IOException {
    int currentEpoch = getAckEpochFromFile();
    String server = msg.getLeave().getServerId();
    // Remove if it's in pendingPeers.
    pendingPeers.remove(server);
    // Remove from quorumSet.
    this.quorumSet.remove(server);
    ClusterConfiguration clusterConfig = getLastSeenConfig();
    Zxid version = clusterConfig.getVersion();
    if (version.getEpoch() != currentEpoch) {
      clusterConfig.setVersion(new Zxid(currentEpoch, 0));
    } else {
      clusterConfig.setVersion(new Zxid(currentEpoch, version.getXid() + 1));
    }
    // Remove it from current configuration.
    clusterConfig.removePeer(source);
    Message cop = MessageBuilder.buildCop(clusterConfig);
    Message remove = MessageBuilder.buildRemoveFollower(server);
    MessageTuple req = new MessageTuple(null, remove);
    // Broadcasts new configuration to all the peers.
    preProcessor.processRequest(new MessageTuple(null, cop));
    // Remove this server from PreProcessor.
    preProcessor.processRequest(req);
    // Remove this server from AckProcessor.
    ackProcessor.processRequest(req);
    return clusterConfig;
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

  /* -----------------   For FOLLOWING state only -------------------- */

  /**
   * Begins executing follower steps. It returns if any exception is caught,
   * which causes it goes back to election phase.
   * @throws Exception
   */
  void follow() throws Exception {
    try {
      /* -- Discovering phase -- */
      changePhase(Phase.DISCOVERING);
      sendProposedEpoch();
      waitForNewEpoch();

      /* -- Synchronizing phase -- */
      changePhase(Phase.SYNCHRONIZING);
      waitForSync(this.electedLeader);
      waitForNewLeaderMessage();
      waitForCommitMessage();
      // Delivers all transactions in log before entering broadcasting phase.
      deliverUndeliveredTxns();

      /* -- Broadcasting phase -- */
      changePhase(Phase.BROADCASTING);
      beginAccepting();
    } catch (InterruptedException e) {
      LOG.debug("Participant is canceled by user.");
      throw e;
    } catch (TimeoutException e) {
      LOG.debug("Didn't hear message from {} for {} milliseconds. Going"
                + " back to leader election.",
                this.electedLeader,
                this.config.getTimeout());
    } catch (BackToElectionException e) {
      LOG.debug("Got GO_BACK message from queue, going back to electing.");
    } catch (QuorumZab.SimulatedException e) {
      LOG.debug("Got SimulatedException, go back to leader election.");
    } catch (Exit e) {
      LOG.debug("Exit running : {}", e.getMessage());
      throw e;
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      throw e;
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
                      .buildProposedEpoch(getProposedEpochFromFile(),
                                          getAckEpochFromFile(),
                                          getLastSeenConfig());
    sendMessage(this.electedLeader, message);
  }

  /**
   * Waits until receives the NEWEPOCH message from leader.
   *
   * @throws InterruptedException if anything wrong happens.
   * @throws TimeoutException in case of timeout.
   * @throws IOException in case of IO failure.
   */
  void waitForNewEpoch()
      throws InterruptedException, TimeoutException, IOException {
    MessageTuple tuple = getExpectedMessage(MessageType.NEW_EPOCH,
                                            this.electedLeader);
    Message msg = tuple.getMessage();
    String source = tuple.getServerId();
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
  void waitForNewLeaderMessage()
      throws TimeoutException, InterruptedException, IOException {
    LOG.debug("Waiting for New Leader message from {}.", this.electedLeader);
    MessageTuple tuple = getExpectedMessage(MessageType.NEW_LEADER,
                                            this.electedLeader);
    Message msg = tuple.getMessage();
    String source = tuple.getServerId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Got NEW_LEADER message from {} : {}.",
                source,
                TextFormat.shortDebugString(msg));
    }
    ZabMessage.NewLeader nl = msg.getNewLeader();
    int epoch = nl.getEpoch();
    // Sync Ack epoch to disk.
    this.log.sync();
    setAckEpoch(epoch);
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
   * @throws ExecutionException in case of exception from executors.
   */
  void beginAccepting()
      throws TimeoutException, InterruptedException, IOException,
      ExecutionException {
    SyncProposalProcessor syncProcessor =
        new SyncProposalProcessor(this.log, this.transport,
                                  SYNC_MAX_BATCH_SIZE);
    CommitProcessor commitProcessor
      = new CommitProcessor(stateMachine, this.lastDeliveredZxid);
    // The last time of HEARTBEAT message comes from leader.
    long lastHeartbeatTime = System.nanoTime();
    int ackEpoch = getAckEpochFromFile();
    this.stateMachine.following(this.electedLeader);
    try {
      while (true) {
        MessageTuple tuple = getMessage();
        Message msg = tuple.getMessage();
        String source = tuple.getServerId();
        if (msg.getType() == MessageType.QUERY_LEADER) {
          LOG.debug("Got QUERY_LEADER from {}", source);
          Message reply = MessageBuilder.buildQueryReply(this.electedLeader);
          sendMessage(source, reply);
          continue;
        }
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
          if (!source.equals(this.serverId)) {
            LOG.debug("Got unexpected message from {}, ignores.", source);
            continue;
          }
        }
        if (msg.getType() == MessageType.PROPOSAL) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Got PROPOSAL {}.", TextFormat.shortDebugString(msg));
          }
          Transaction txn = MessageBuilder.fromProposal(msg.getProposal());
          Zxid zxid = txn.getZxid();
          if (zxid.getEpoch() == ackEpoch) {
            // Dispatch to SyncProposalProcessor and CommitProcessor.
            syncProcessor.processRequest(tuple);
            commitProcessor.processRequest(tuple);
          } else {
            LOG.debug("The proposal has the wrong epoch number {}.",
                      zxid.getEpoch());
            throw new RuntimeException("The proposal has wrong epoch number.");
          }
        } else if (msg.getType() == MessageType.COMMIT) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Got COMMIT {}.", TextFormat.shortDebugString(msg));
          }
          commitProcessor.processRequest(tuple);
        } else if (msg.getType() == MessageType.HEARTBEAT) {
          LOG.trace("Got HEARTBEAT from {}.", source);
          // Replies HEARTBEAT message to leader.
          Message heartbeatReply = MessageBuilder.buildHeartbeat();
          sendMessage(source, heartbeatReply);
        } else if (msg.getType() == MessageType.COP) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Got COP {}", TextFormat.shortDebugString(msg));
          }
          ClusterConfiguration cnf =
            ClusterConfiguration.fromProto(msg.getConfig(), this.serverId);
          setLastSeenConfig(cnf);
          Message ackCop = MessageBuilder.buildAckCop(cnf.getVersion());
          sendMessage(this.electedLeader, ackCop);
          if (!getLastSeenConfig().contains(this.serverId)) {
            LOG.debug("The server is not in the new configuration, exiting.");
            throw new Exit("Due to the server is not in new configuration");
          }
        } else {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Unexpected messgae : {} from {}",
                     TextFormat.shortDebugString(msg),
                     source);
          }
        }
      }
    } finally {
      commitProcessor.shutdown();
      syncProcessor.shutdown();
      this.lastDeliveredZxid = commitProcessor.getLastDeliveredZxid();
    }
  }

  void join(String peer) throws Exception {
    LOG.debug("Trying to join server {}.", this.config.getJoinPeer());
    if (peer.equals(this.serverId)) {
      // If the server joins itself, means the cluster starts from itself, goes
      // to broacasting phase directly.
      try {
        changeState(State.LEADING);
        List<String> servers = new ArrayList<String>();
        servers.add(this.serverId);
        ClusterConfiguration cnf = new ClusterConfiguration(Zxid.ZXID_NOT_EXIST,
                                                            servers,
                                                            this.serverId);
        setLastSeenConfig(cnf);
        setAckEpoch(0);
        setProposedEpoch(0);

        /* -- Broadcasting phase -- */
        changePhase(Phase.BROADCASTING);
        beginBroadcasting();
      } catch (InterruptedException e) {
        LOG.debug("Participant is canceled by user.");
        throw e;
      } catch (TimeoutException e) {
        LOG.debug("Didn't hear message from peers for {} milliseconds. Going"
                  + " back to leader election.",
                  this.config.getTimeout());
      } catch (BackToElectionException e) {
        LOG.debug("Got GO_BACK message from queue, going back to electing.");
      } catch (Exit e) {
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
    } else {
      try {
        changeState(State.FOLLOWING);
        LOG.debug("Query leader from {}", peer);
        Message query = MessageBuilder.buildQueryLeader();
        sendMessage(peer, query);
        MessageTuple tuple = getExpectedMessage(MessageType.QUERY_LEADER_REPLY,
                                                peer);
        this.electedLeader = tuple.getMessage().getReply().getLeader();
        LOG.debug("Got current leader {}", this.electedLeader);
        Message join = MessageBuilder.buildJoin();
        sendMessage(this.electedLeader, join);

        /* -- Synchronizing phase --*/
        changePhase(Phase.SYNCHRONIZING);
        waitForSync(this.electedLeader);
        waitForNewLeaderMessage();
        waitForCommitMessage();
        setProposedEpoch(getAckEpochFromFile());
        // Delivers all transactions in log before entering broadcasting phase.
        deliverUndeliveredTxns();

        /* -- Broadcasting phase -- */
        changePhase(Phase.BROADCASTING);
        beginAccepting();
      } catch (InterruptedException e) {
        LOG.debug("Participant is canceled by user.");
        throw e;
      } catch (TimeoutException e) {
        LOG.debug("Didn't hear message from {} for {} milliseconds. Going"
                  + " back to leader election.",
                  this.electedLeader,
                  this.config.getTimeout());
        if (getLastSeenConfig() == null) {
          throw new JoinFailure("Fails to join cluster.");
        }
      } catch (BackToElectionException e) {
        LOG.debug("Got GO_BACK message from queue, going back to electing.");
        if (getLastSeenConfig() == null) {
          throw new JoinFailure("Fails to join cluster.");
        }
      } catch (Exit e) {
        LOG.debug("Exit running : {}", e.getMessage());
        throw e;
      } catch (Exception e) {
        LOG.error("Caught exception", e);
        throw e;
      } finally {
        if (this.electedLeader != null) {
          this.transport.clear(this.electedLeader);
        }
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
    private final ClusterConfiguration clusterConfig;

   /**
    * Constructs the SyncPeerTask object.
    *
    * @param peerId the id of the peer.
    * @param peerLatestZxid the last zxid of the peer.
    * @param lastSyncZxid leader will synchronize the follower up to this zxid.
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
      LOG.debug("Begins synchronizing history to {}(last zxid : {})", peerId,
                peerLatestZxid);
      Zxid syncPoint = null;
      if (this.lastSyncZxid.getEpoch() == peerLatestZxid.getEpoch()) {
        // If the peer has same epoch number as the server.
        if (this.lastSyncZxid.compareTo(peerLatestZxid) >= 0) {
          // Means peer's history is the prefix of the server's.
          LOG.debug("{}'s history >= {}'s, sending DIFF.", serverId, peerId);
          syncPoint = new Zxid(peerLatestZxid.getEpoch(),
                               peerLatestZxid.getXid() + 1);
          Message diff = MessageBuilder.buildDiff(this.lastSyncZxid);
          sendMessage(peerId, diff);
        } else {
          // Means peer's history is the superset of the server's.
          LOG.debug("{}'s history < {}'s, sending TRUNCATE.", serverId,
                    peerId);
          // Doesn't need to synchronize anything, just truncate.
          syncPoint = null;
          Message trunc = MessageBuilder.buildTruncate(this.lastSyncZxid);
          sendMessage(peerId, trunc);
        }
      } else {
        // They have different epoch numbers. Truncate all.
        LOG.debug("The last epoch of {} and {} are different, sending "
                  + "SNAPSHOT.", serverId, peerId);
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
      // At the end of the synchronization, send COP.
      Message syncEnd = MessageBuilder.buildSyncEnd(this.clusterConfig);
      sendMessage(peerId, syncEnd);
    }
  }
}
