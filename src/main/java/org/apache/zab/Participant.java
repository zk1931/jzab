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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;

/**
 * Participant.
 */
public class Participant implements Runnable,
                                    Transport.Receiver,
                                    ServerState,
                                    Election.ElectionCallback {
  /**
   * Used for communication between nodes.
   */
  protected Transport transport;

  /**
   * Callback interface for testing purpose.
   */
  protected StateChangeCallback stateChangeCallback = null;

  /**
   * Message queue. The receiving callback simply parses the message and puts
   * it in queue, it's up to leader/follower/election module to take out
   * the message.
   */
  protected BlockingQueue<MessageTuple> messageQueue =
    new LinkedBlockingQueue<MessageTuple>();

  /**
   * The transaction log.
   */
  protected Log log;

  /**
   * Configuration of Zab.
   */
  protected ZabConfig config;

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
  String electedLeader = null;

  /**
   * Used for leader election.
   */
  CountDownLatch leaderCondition;

  private static final Logger LOG = LoggerFactory.getLogger(Participant.class);

  public Participant(ZabConfig config, StateMachine stateMachine)
      throws IOException {
    this.config = config;
    this.stateMachine = stateMachine;
    this.fAckEpoch = new File(config.getLogDir(), "AckEpoch");
    this.fProposedEpoch = new File(config.getLogDir(), "ProposedEpoch");


    // Transaction log file.
    File logFile = new File(this.config.getLogDir(), "transaction.log");
    this.log = new SimpleLog(logFile);

    // Constructs transport.
    this.transport = new DummyTransport(this.config.getServerId(), this);
  }

  Participant(StateMachine stateMachine,
              StateChangeCallback cb,
              TestState initialState) throws IOException {

    this.config = new ZabConfig(initialState.prop);
    this.stateMachine = stateMachine;
    this.fAckEpoch = new File(config.getLogDir(), "AckEpoch");
    this.fProposedEpoch = new File(config.getLogDir(), "ProposedEpoch");

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
    this.transport = new DummyTransport(this.config.getServerId(), this);
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

  @Override
  public void onReceived(String source, ByteBuffer message) {
    byte[] buffer = null;

    try {
      // Parses it to protocol message.
      buffer = new byte[message.remaining()];
      message.get(buffer);
      Message msg = Message.parseFrom(buffer);

      if (LOG.isDebugEnabled()) {
        LOG.debug("{} received message from {}: {} ",
                  this.config.getServerId(),
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
  public void run() {

    try {

      if (this.stateChangeCallback != null) {
        this.stateChangeCallback.electing();
      }

      Election electionAlg = new RoundRobinElection();
      startLeaderElection(electionAlg);
      waitLeaderElected();

      LOG.debug("{} selected {} as prospective leader.",
                this.config.getServerId(),
                this.electedLeader);

      if (this.electedLeader.equals(this.config.getServerId())) {
        lead();
      } else {
        follow();
      }
    } catch (Exception e) {
      LOG.warn("Caught election ", e);
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
   * Gets the expected message. It will discard unexpected messages and
   * returns only if the expected message is received.
   *
   * @param type the expected message type.
   * @return the message tuple contains the message and its source.
   * @throws TimeoutException in case of timeout.
   * @throws InterruptedException it's interrupted.
   */
  MessageTuple getExpectedMessage(MessageType type)
      throws TimeoutException, InterruptedException {
    // Waits until the expected message is received.
    while (true) {
      MessageTuple tuple = getMessage();
      if (tuple.getMessage().getType() == type) {
        return tuple;
      } else if (LOG.isDebugEnabled()) {
        LOG.debug("Got an unexpected message from {}: {}",
                  tuple.getSource(),
                  TextFormat.shortDebugString(tuple.getMessage()));
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
    transport.send(dest, ByteBuffer.wrap(message.toByteArray()));
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

  /* -----------------   For LEADING state only -------------------- */

  /**
   * Begins executing leader steps. It returns if any exception is caught,
   * which causes it goes back to election phase.
   */
  void lead() {

    Map<String, FollowerStat> quorumSet = new HashMap<String, FollowerStat>();

    try {

      /* -- Discovering phase -- */

      if (stateChangeCallback != null) {
        stateChangeCallback.leaderDiscovering(config.getServerId());
      }

      // Gets the proposed message from a quorum.
      getPropsedEpochFromQuorum(quorumSet);

      // Proposes new epoch to followers.
      proposeNewEpoch(quorumSet);

      // Waits the new epoch is established.
      waitEpochAckFromQuorum(quorumSet);

      LOG.debug("Leader {} established new epoch {}!",
                config.getServerId(),
                getProposedEpochFromFile());

      // Finds one who has the "best" history.
      String serverId = selectSyncHistoryOwner(quorumSet);

      LOG.debug("Leader {} chooses {} to pull its history.",
                this.config.getServerId(),
                serverId);


      /* -- Synchronization phase -- */

      if (stateChangeCallback != null) {
        stateChangeCallback.leaderSynchronizating(getProposedEpochFromFile());
      }

      // Pulls history from the server.
      synchronizeFromFollower(serverId);

      synchronizeFollowers();


      /* -- Broadcasting phase -- */

      if (stateChangeCallback != null) {
        stateChangeCallback.leaderBroadcasting(getAckEpochFromFile());
      }

      beginBroadcasting();

    } catch (InterruptedException | TimeoutException | IOException e) {
      LOG.error("Leader caught exception", e);
    }
  }

  /**
   * Waits until receives the CEPOCH message from the quorum.
   *
   * @param quorumSet the quorum set.
   * @throws InterruptedException if anything wrong happens.
   * @throws TimeoutException in case of timeout.
   */
  void getPropsedEpochFromQuorum(Map<String, FollowerStat> quorumSet)
      throws InterruptedException, TimeoutException {
    // Gets last proposed epoch from other servers (not including leader).
    while (quorumSet.size() < getQuorumSize() - 1) {
      MessageTuple tuple = getExpectedMessage(MessageType.PROPOSED_EPOCH);
      Message msg = tuple.getMessage();
      String source = tuple.getSource();
      ProposedEpoch epoch = msg.getProposedEpoch();

      if (quorumSet.containsKey(source)) {
        throw new RuntimeException("Quorum set has already contained "
            + source + ", probably a bug?");
      }
      quorumSet.put(source, new FollowerStat(epoch.getProposedEpoch()));
    }
    LOG.debug("{} got proposed epoch from a quorum.", config.getServerId());
  }

  /**
   * Finds an epoch number which is higher than any proposed epoch in quorum
   * set and propose the epoch to them.
   *
   * @param quorumSet the quorum set.
   * @throws IOException in case of IO failure.
   */
  void proposeNewEpoch(Map<String, FollowerStat> quorumSet) throws IOException {
    List<Integer> epochs = new ArrayList<Integer>();

    // Puts leader's last received proposed epoch in list.
    epochs.add(getProposedEpochFromFile());

    for (FollowerStat stat : quorumSet.values()) {
      epochs.add(stat.getLastProposedEpoch());
    }

    int newEpoch = Collections.max(epochs) + 1;

    // Updates itself's last proposed epoch.
    setProposedEpoch(newEpoch);

    LOG.debug("{} begins proposing new epoch {}",
              config.getServerId(),
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
  void waitEpochAckFromQuorum(Map<String, FollowerStat> quorumSet)
      throws InterruptedException, TimeoutException {

    int ackCount = 0;

    // Waits the Ack from all other peers in the quorum set.
    while (ackCount < quorumSet.size()) {
      MessageTuple tuple = getExpectedMessage(MessageType.ACK_EPOCH);
      Message msg = tuple.getMessage();
      String source = tuple.getSource();

      if (!quorumSet.containsKey(source)) {
        throw new RuntimeException("The message is not from quorum set,"
            + "probably a bug?");
      }

      ackCount++;

      AckEpoch ackEpoch = msg.getAckEpoch();
      ZabMessage.Zxid zxid = ackEpoch.getLastZxid();

      // Updates follower's f.a and lastZxid.
      FollowerStat fs = quorumSet.get(source);
      fs.setLastAckedEpoch(ackEpoch.getAcknowledgedEpoch());
      fs.setLastProposedZxid(new Zxid(zxid.getEpoch(), zxid.getXid()));
    }

    LOG.debug("{} received ACKs from the quorum set of size {}.",
              config.getServerId(),
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
  String selectSyncHistoryOwner(Map<String, FollowerStat> quorumSet)
      throws IOException {
    // Starts finding the server who owns the longest transaction history,
    // starts from leader itself.
    int ackEpoch = getAckEpochFromFile();
    Zxid zxid = new Zxid(0, 0);
    String serverId = config.getServerId();

    Iterator<Map.Entry<String, FollowerStat>> iter;
    iter = quorumSet.entrySet().iterator();

    while (iter.hasNext()) {
      Map.Entry<String, FollowerStat> entry = iter.next();

      int fEpoch = entry.getValue().getLastAckedEpoch();
      Zxid fZxid = entry.getValue().getLastProposedZxid();

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
  void synchronizeFromFollower(String serverId) throws IOException {
    LOG.debug("Begins sync from follower {}.", serverId);

    //Message pullTxn = MessageBuilder.buildPullTxnReq(log.getLatestZxid());
  }

  /**
   * Synchronizes leader's history to followers.
   */
  void synchronizeFollowers() {
    // Not implemented yet.
  }

  /**
   * Entering broadcasting phase, leader broadcasts proposal to
   * followers.
   */
  void beginBroadcasting() {
    // Not implemented yet.
  }


  /* -----------------   For FOLLOWING state only -------------------- */

  /**
   * Begins executing follower steps. It returns if any exception is caught,
   * which causes it goes back to election phase.
   */
  void follow() {

    try {

      /* -- Discovering phase -- */

      if (stateChangeCallback != null) {
        stateChangeCallback.followerDiscovering(this.electedLeader);
      }

      sendProposedEpoch();

      receiveNewEpoch();


      /* -- Synchronization phase -- */

      if (stateChangeCallback != null) {
        stateChangeCallback.followerSynchronizating(getProposedEpochFromFile());
      }

      synchronization();


      /* -- Broadcasting phase -- */

      if (stateChangeCallback != null) {
        stateChangeCallback.followerBroadcasting(getAckEpochFromFile());
      }

      beginAccepting();

    } catch (InterruptedException | TimeoutException | IOException e) {
      LOG.error("Follower caught exception.", e);
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

    MessageTuple tuple = getExpectedMessage(MessageType.NEW_EPOCH);
    Message msg = tuple.getMessage();
    String source = tuple.getSource();

    if (!source.equals(this.electedLeader)) {
      throw new RuntimeException("The new epoch message is not from current"
          + "leader, maybe a bug?");
    }

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

    LOG.debug("{} receives the new epoch proposal {} from {}.",
              config.getServerId(),
              epoch.getNewEpoch(),
              source);

    Zxid zxid = log.getLatestZxid();
    LOG.debug("Get last zxid : {}", zxid);

    // Sends ACK to leader.
    sendMessage(this.electedLeader,
                MessageBuilder.buildAckEpoch(getAckEpochFromFile(),
                                             zxid));
  }

  /**
   * Entering synchronization phase.
   */
  void synchronization() {
    // Not implemented yet.
  }

  /**
   * Entering broadcasting phase.
   */
  void beginAccepting() {
    // Not implemented yet.
  }
}
