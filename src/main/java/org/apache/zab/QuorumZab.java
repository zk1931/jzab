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

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.zab.proto.ZabMessage;
import org.apache.zab.proto.ZabMessage.AckEpoch;
import org.apache.zab.proto.ZabMessage.Message;
import org.apache.zab.proto.ZabMessage.NewEpoch;
import org.apache.zab.proto.ZabMessage.ProposedEpoch;
import org.apache.zab.transport.Transport;
import org.apache.zab.transport.DummyTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.zab.proto.ZabMessage.Message.MessageType;;

/**
 * Quorum zab implementation. This class manages the quorum zab servers.
 * For each server, it can be in one of three states :
 *   ELECTING, FOLLOWING, LEADING.
 * Among all the nodes, there's only one server can be established leader.
 */
public class QuorumZab extends Zab implements ServerState,
                                              Transport.Receiver,
                                              Runnable {

  /**
   * Used for communication between nodes.
   */
  protected Transport transport;

  /**
   * The last received proposed epoch. Corresponds to f.p in Zab paper.
   */
  protected int proposedEpoch;

  /**
   * The last acknowledged epoch received. Corresponds to f.a in Zab paper.
   */
  protected int acknowledgedEpoch;

  /**
   * Callback interface for testing purpose.
   */
  protected StateChangeCallback stateChangeCallback;

  /**
   * Message queue. The receiving callback simply parses the message and puts
   * it in queue, it's up to leader/follower/election module to take out
   * the message.
   */
  protected BlockingQueue<MessageTuple> messageQueue;

  private static final Logger LOG = LoggerFactory.getLogger(QuorumZab.class);

  {
    proposedEpoch = -1;
    acknowledgedEpoch = -1;
    messageQueue = new LinkedBlockingQueue<MessageTuple>();
    stateChangeCallback = null;
  }

  /**
   * A tuple holds both message and its source.
   */
  static class MessageTuple {
    final String source;
    final Message message;

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

  public QuorumZab(StateMachine stateMachine, Properties prop) {
    super(stateMachine, prop);
    this.transport = new DummyTransport(this.config.getServerId(), this);
    Executors.newSingleThreadExecutor(DaemonThreadFactory.FACTORY)
             .execute(this);
  }

  QuorumZab(StateMachine stateMachine,
            Properties prop,
            StateChangeCallback cb,
            TestState initialState) {
    super(stateMachine, prop);
    this.stateChangeCallback = cb;
    this.proposedEpoch = initialState.getProposedEpoch();
    this.acknowledgedEpoch = initialState.getAckEpoch();
    this.transport = new DummyTransport(this.config.getServerId(), this);
    Executors.newSingleThreadExecutor(DaemonThreadFactory.FACTORY)
             .execute(this);
  }

  @Override
  public void send(ByteBuffer message) throws IOException {
  }

  @Override
  public void trimLogTo(Zxid zxid) {
  }

  @Override
  public void replayLogFrom(Zxid zxid) throws IOException {
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
   * Gets last received proposed epoch number.
   *
   * @return last received proposed epoch number
   */
  @Override
  public int getProposedEpoch() {
    return this.proposedEpoch;
  }

  /**
   * Callback which will be called from transport layer when a message is
   * received. This callback may be called from different thread for different
   * sources, so we should be aware of the race condition.
   *
   * @param source the source of the message.
   * @param message the ByteBuffer which contains the protocol buffer message.
   */
  @Override
  public void onReceived(String source, ByteBuffer message) {

    byte[] buffer = null;

    try {
      // Parses it to protocol message.
      buffer = new byte[message.remaining()];
      message.get(buffer);
      Message msg = Message.parseFrom(buffer);

      LOG.debug("{} received message from {} : {} ",
                this.config.getServerId(),
                source,
                msg);

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

  /**
   * Callback which will be called when transport detects the connection is
   * disconnected.
   *
   * @param serverId the peer of the connection.s
   */
  @Override
  public void onDisconnected(String serverId) {
  }

  /**
   * Starts main logic of QuorumZab.
   */
  @Override
  public void run() {

    if (this.stateChangeCallback != null) {
      this.stateChangeCallback.electing();
    }

    ElectionEvent event = new ElectionEvent();
    // Initializes the leader election.
    Election election = new RoundRobinElection();
    election.initialize(this, event);

    try {
      // Blocks until the leader is elected.
      String electedLeader = event.getLeader();

      if (electedLeader.equals(this.config.getServerId())) {
        new Leader().lead();
      } else {
        new Follower(electedLeader).follow();
      }

    } catch (Exception e) {
      LOG.error("Interruped exception", e);
    }
  }

  /**
   * Participants of the system. It's the super class of Leader and Follower.
   */
  abstract class Participant {

    /**
     * The file to store the last acknowledged epoch.
     */
    private final File fAckEpoch;

    /**
     * The file to store the last proposed epoch.
     */
    private final File fProposedEpoch;

    Participant() {
      this.fAckEpoch = new File(config.getLogDir(), "AckEpoch");
      this.fProposedEpoch = new File(config.getLogDir(), "ProposedEpoch");
    }

    /**
     * Gets the last acknowledged epoch.
     *
     * @return the last acknowledged epoch.
     * @throws IOException in case of IO failures.
     */
    int getAckEpoch() throws IOException {
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
    int getProposedEpoch() throws IOException {
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
        } else {

          LOG.debug("Got an unexpected message from {}: {}",
                    tuple.getSource(),
                    tuple.getMessage());
        }
      }
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
  }

  /**
   * This class has control logic for leader. Leader plays both leader role
   * and follower role.
   */
  class Leader extends Participant {

    /**
     * Stores the followers of this leader (not including itself).
     */
    final Map<String, FollowerStat> quorumSet;

    {
      quorumSet = new HashMap<String, FollowerStat>();
    }

    /**
     * Begins executing leader steps. It returns if any exception is caught,
     * which causes it goes back to election phase.
     */
    void lead() {

      try {

        /* -- Discovering phase -- */

        if (stateChangeCallback != null) {
          stateChangeCallback.leaderDiscovering(config.getServerId());
        }

        // Gets the proposed message from a quorum.
        getPropsedEpochFromQuorum();

        // Proposes new epoch to followers.
        proposeNewEpoch();

        // Waits the new epoch is established.
        waitEpochAckFromQuorum();

        LOG.debug("Leader {} established new epoch {}!",
                  config.getServerId(),
                  proposedEpoch);

        // Finds one who has the "best" history.
        String serverId = selectSyncHistoryOwner();

        // Pulls history from the server.
        syncrhonizeFromFollower(serverId);


        /* -- Synchronization phase -- */

        if (stateChangeCallback != null) {
          stateChangeCallback.leaderSynchronizating(proposedEpoch, serverId);
        }

        synchronizeFollowers();


        /* -- Broadcasting phase -- */

        if (stateChangeCallback != null) {
          stateChangeCallback.leaderBroadcasting(acknowledgedEpoch);
        }

        beginBroadcasting();

      } catch (InterruptedException | TimeoutException e) {
        LOG.error("Leader caught exception", e);
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
     */
    void proposeNewEpoch() {
      List<Integer> epochs = new ArrayList<Integer>();

      // Puts leader's last received proposed epoch in list.
      epochs.add(proposedEpoch);

      for (FollowerStat stat : quorumSet.values()) {
        epochs.add(stat.getLastProposedEpoch());
      }

      int newEpoch = Collections.max(epochs) + 1;

      // Updates itself's last proposed epoch.
      proposedEpoch = newEpoch;

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
     * @throws InterruptedException if anything wrong happens.
     * @throws TimeoutException in case of timeout.
     */
    void waitEpochAckFromQuorum()
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
     * @return the id of the server
     */
    String selectSyncHistoryOwner() {
      // Starts finding the server who owns the longest transaction history,
      // starts from leader itself.
      int ackEpoch = acknowledgedEpoch;
      Zxid zxid = new Zxid(0, 0);
      String serverId = config.getServerId();

      Iterator<Map.Entry<String, FollowerStat>> iter;
      iter = quorumSet.entrySet().iterator();

      while (iter.hasNext()) {
        Map.Entry<String, FollowerStat> entry = iter.next();

        int fAckEpoch = entry.getValue().getLastAckedEpoch();
        Zxid fZxid = entry.getValue().getLastProposedZxid();

        if (fAckEpoch > ackEpoch ||
            (fAckEpoch == ackEpoch && fZxid.compareTo(zxid) > 0)) {
          ackEpoch = fAckEpoch;
          zxid = fZxid;
          serverId = entry.getKey();
        }
      }

      LOG.debug("{} has largest acknowledged epoch {} and longest history {}",
                serverId, ackEpoch, zxid);

      return serverId;
    }

    /**
     * Pulls the history from the server who has the "best" history.
     *
     * @param serverId the id of the server whose history is selected.
     */
    void syncrhonizeFromFollower(String serverId) {
      // Not implemented yet.
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
  }

  /**
   * This class has control logic for follower.
   */
  class Follower extends Participant {

    protected String electedLeader;

    public Follower(String electedLeader) {
      this.electedLeader = electedLeader;
    }

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

        // If required, synchronizes its history to leader.
        synchronizeToLeader();


        /* -- Synchronization phase -- */

        if (stateChangeCallback != null) {
          stateChangeCallback.followerSynchronizating(proposedEpoch);
        }

        synchronizeFromLeader();


        /* -- Broadcasting phase -- */

        if (stateChangeCallback != null) {
          stateChangeCallback.followerBroadcasting(acknowledgedEpoch);
        }

        beginAccepting();

      } catch (InterruptedException | TimeoutException e) {
        LOG.error("Follower caught exception.", e);
      }
    }

    /**
     * Sends CEPOCH message to its prospective leader.
     */
    void sendProposedEpoch() {
      Message message = MessageBuilder.buildProposedEpoch(proposedEpoch);
      sendMessage(this.electedLeader, message);
    }

    /**
     * Waits until receives the NEWEPOCH message from leader.
     *
     * @throws InterruptedException if anything wrong happens.
     * @throws TimeoutException in case of timeout.
     */
    void receiveNewEpoch() throws InterruptedException, TimeoutException {
      MessageTuple tuple = getExpectedMessage(MessageType.NEW_EPOCH);
      Message msg = tuple.getMessage();
      String source = tuple.getSource();

      if (!source.equals(this.electedLeader)) {
        throw new RuntimeException("The new epoch message is not from current"
            + "leader, maybe a bug?");
      }

      NewEpoch epoch = msg.getNewEpoch();
      if (epoch.getNewEpoch() < proposedEpoch) {
        LOG.warn("New epoch is smaller than last received proposed epoch");
        throw new RuntimeException("New epoch is smaller than current one.");
      }

      // Updates follower's last proposed epoch.
      proposedEpoch = epoch.getNewEpoch();

      LOG.debug("{} receives the new epoch proposal {} from {}.",
                config.getServerId(),
                epoch.getNewEpoch(),
                source);

      // Sends ACK to leader.
      sendMessage(this.electedLeader,
                  MessageBuilder.buildAckEpoch(acknowledgedEpoch,
                                               new Zxid(0, 0)));
    }

    /**
     * The follower who has longest history will synchronize its history to
     * leader.
     */
    void synchronizeToLeader() {
      // Not implemented yet.
    }

    /**
     * Entering synchronization phase, follower synchronizes from leader.
     */
    void synchronizeFromLeader() {
      // Not implemented yet.
    }

    /**
     * Entering broadcasting phase.
     */
    void beginAccepting() {
      // Not implemented yet.
    }
  }

  /**
   * Stores the statistics about follower.
   */
  protected static class FollowerStat {
    private int lastProposedEpoch = -1;
    private int lastAckedEpoch = -1;
    private long lastHeartbeatTime;
    private Zxid lastProposedZxid = null;

    public FollowerStat(int epoch) {
      setLastProposedEpoch(epoch);
      updateHeartbeatTime();
    }

    long getLastHeartbeatTime() {
      return this.lastHeartbeatTime;
    }

    void updateHeartbeatTime() {
      this.lastHeartbeatTime = System.nanoTime();
    }

    int getLastProposedEpoch() {
      return this.lastProposedEpoch;
    }

    void setLastProposedEpoch(int epoch) {
      this.lastProposedEpoch = epoch;
    }

    int getLastAckedEpoch() {
      return this.lastAckedEpoch;
    }

    void setLastAckedEpoch(int epoch) {
      this.lastAckedEpoch = epoch;
    }

    Zxid getLastProposedZxid() {
      return this.lastProposedZxid;
    }

    void setLastProposedZxid(Zxid zxid) {
      this.lastProposedZxid = zxid;
    }
  }

  /**
   * Interface of callbacks which will be called when phase change happens.
   * Used for testing purpose.
   *
   * Phase changes :
   *
   *        leaderDiscovering - leaderSynchronizating - leaderBroadcasting
   *        /
   * electing
   *        \
   *        followerDiscovering - followerSynchronizating - followerBroadcasting
   *
   */
  interface StateChangeCallback {

    /**
     * Will be called when entering electing phase.
     */
    void electing();

    /**
     * Will be called when entering discovering phase of leader.
     *
     * @param electedLeader the elected leader.
     */
    void leaderDiscovering(String electedLeader);

    /**
     * Will be called when entering discovery phase of follower.
     *
     * @param electedLeader the elected leader of this follower.
     */
    void followerDiscovering(String electedLeader);

    /**
     * Will be called when entering synchronization phase of leader.
     *
     * @param epoch the established epoch.
     * @param server the id of the server whose history is selected for
     * synchronization.
     */
    void leaderSynchronizating(int epoch, String server);

    /**
     * Will be called when entering synchronization phase of follower.
     *
     * @param epoch the established epoch.
     */
    void followerSynchronizating(int epoch);

    /**
     * Will be called when entering broadcasting phase of leader.
     *
     * @param epoch the acknowledged epoch (f.a).
     */
    void leaderBroadcasting(int epoch);

    /**
     * Will be called when entering broadcasting phase of follower.
     *
     * @param epoch the current epoch (f.a).
     */
    void followerBroadcasting(int epoch);
  }

  /**
   * Used for initializing the state of QuorumZab for testing purpose.
   */
  static class TestState {

    int proposedEpoch = -1;

    int acknowledgedEpoch = -1;

    TestState setProposedEpoch(int epoch) {
      this.proposedEpoch = epoch;
      return this;
    }

    TestState setAckEpoch(int epoch) {
      this.acknowledgedEpoch = epoch;
      return this;
    }

    int getProposedEpoch() {
      return this.proposedEpoch;
    }

    int getAckEpoch() {
      return this.acknowledgedEpoch;
    }
  }
}
