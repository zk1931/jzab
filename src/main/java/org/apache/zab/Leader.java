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
import java.nio.ByteBuffer;
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
          LOG.debug("Got DISCONNECTED in getMessage().",
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
    transport.broadcast(peers, ByteBuffer.wrap(message.toByteArray()));
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
    LOG.debug("Begins broadcasting!");
    Log log = persistence.getLog();
    Zxid lastZxid = log.getLatestZxid();
    // Last committed zxid main thread has received.
    Zxid lastCommittedZxid = lastZxid;
    int currentEpoch = persistence.getAckEpoch();
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
                                                 this.persistence,
                                                 lastZxid);
    SyncProposalProcessor syncProcessor =
        new SyncProposalProcessor(log, this.transport,
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
    // Call cluster change right before the leading callback.
    this.stateMachine.clusterChange(
        new HashSet<String>(persistence.getLastSeenConfig().getPeers()));
    // First time call leading callback.
    notifyQuorumSetChange();
    try {
      while (true) {
        if (pendingConfig != null) {
          if (this.quorumSet.size() < pendingConfig.getQuorumSize()) {
            break;
          }
        } else if (this.quorumSet.size() < getQuorumSize()){
          break;
        }
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
          Message newEpoch = MessageBuilder.buildNewEpochMessage(currentEpoch);
          sendMessage(source, newEpoch);
        } else if (msg.getType() == MessageType.ACK_EPOCH) {
          LOG.debug("Got ACK_EPOCH from {}", source);
          onAckEpoch(tuple, preProcessor);
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
          pendingConfig = onJoin(tuple, preProcessor);
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
            onAck(tuple, ackProcessor, lastCommittedZxid, pendingPeers);
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
            onFlushSyncProcessor(tuple, pendingPeers, ackProcessor);
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
            onCommit(tuple, commitProcessor, pendingPeers);
          } else if (msg.getType() == MessageType.DISCONNECTED) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got DISCONNECTED message {}",
                        TextFormat.shortDebugString(msg));
            }
            onDisconnected(tuple, pendingPeers, preProcessor, ackProcessor);
          } else if (msg.getType() == MessageType.COP) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got message COP {}", TextFormat.shortDebugString(msg));
            }
            onCop(tuple);
          } else if (msg.getType() == MessageType.LEAVE) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Got message LEAVE {}",
                        TextFormat.shortDebugString(msg));
            }
            if (pendingConfig != null) {
              LOG.warn("There's a pending configuration, ignore LEAVE request");
              continue;
            }
            pendingConfig = onLeave(tuple, pendingPeers, preProcessor,
                                    ackProcessor);
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
                  LOG.debug("{} has been removed from cluster.", this.serverId);
                  throw new LeftCluster("Server has been removed from cluster");
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
      this.participantState.updateLastDeliveredZxid(this.lastDeliveredZxid);
    }
  }

  ClusterConfiguration onJoin(MessageTuple tuple,
                              PreProcessor preProcessor) throws IOException {
    String source = tuple.getServerId();
    PeerHandler ph = new PeerHandler(source,
                                     this.transport,
                                     this.config.getTimeout() / 3);
    ph.setLastZxid(Zxid.ZXID_NOT_EXIST);
    addToQuorumSet(source, ph);
    Message flush = MessageBuilder.buildFlushPreProcessor(source);
    Message add = MessageBuilder.buildAddFollower(source);
    ClusterConfiguration clusterConfig = persistence.getLastSeenConfig();
    int currentEpoch = persistence.getAckEpoch();
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

  void onFlushSyncProcessor(MessageTuple tuple,
                            Map<String, PeerHandler> pendingPeers,
                            AckProcessor ackProcessor) throws IOException {
    Message msg = tuple.getMessage();
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
                                    persistence.getLastSeenConfig()),
                   persistence.getAckEpoch());
    ph.startSynchronizingTask();
    pendingPeers.put(followerId, ph);
  }

  void onCommit(MessageTuple tuple,
                CommitProcessor commitProcessor,
                Map<String, PeerHandler> pendingPeers) {
    Message msg = tuple.getMessage();
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
    commitProcessor.processRequest(tuple);
  }

  void onDisconnected(MessageTuple tuple,
                      Map<String, PeerHandler> pendingPeers,
                      PreProcessor preProcessor,
                      AckProcessor ackProcessor) {
    Message msg = tuple.getMessage();
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
    removeFromQuorumSet(followerId);
    Message remove = MessageBuilder.buildRemoveFollower(followerId);
    MessageTuple req = new MessageTuple(null, remove);
    // Ask PreProcessor to remove this follower.
    preProcessor.processRequest(req);
    // Ask AckProcessor to remove this follower.
    ackProcessor.processRequest(req);
  }

  void onAck(MessageTuple tuple,
             AckProcessor ackProcessor,
             Zxid lastCommittedZxid,
             Map<String, PeerHandler> pendingPeers) {
    String source = tuple.getServerId();
    Message msg = tuple.getMessage();
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
    ackProcessor.processRequest(tuple);
  }

  void onAckEpoch(MessageTuple tuple,
                  PreProcessor preProcessor) {
    String source = tuple.getServerId();
    Message msg = tuple.getMessage();
    ZabMessage.AckEpoch ackEpoch = msg.getAckEpoch();
    Zxid lastPeerZxid = MessageBuilder
                        .fromProtoZxid(ackEpoch.getLastZxid());
    PeerHandler ph = new PeerHandler(source,
                                     this.transport,
                                     this.config.getTimeout() / 3);
    ph.setLastZxid(lastPeerZxid);
    addToQuorumSet(source, ph);
    Message flush = MessageBuilder.buildFlushPreProcessor(source);
    Message add = MessageBuilder.buildAddFollower(source);
    // Flush the pipeline before start synchronization.
    preProcessor.processRequest(new MessageTuple(null, flush));
    // Add new joined follower to PreProcessor.
    preProcessor.processRequest(new MessageTuple(null, add));
  }

  ClusterConfiguration onLeave(MessageTuple tuple,
                               Map<String, PeerHandler> pendingPeers,
                               PreProcessor preProcessor,
                               AckProcessor ackProcessor)
      throws IOException {
    String source = tuple.getServerId();
    Message msg = tuple.getMessage();
    int currentEpoch = persistence.getAckEpoch();
    String server = msg.getLeave().getServerId();
    // Remove from quorumSet.
    removeFromQuorumSet(server);
    ClusterConfiguration clusterConfig = persistence.getLastSeenConfig();
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

  /**
   * Notify the clients of the changes for the quorum set. This happens
   * when somebody gets disconnected, recovered, joined or removed.
   */
  private void notifyQuorumSetChange() {
    this.stateMachine.leading(new HashSet<String>(this.quorumSet.keySet()));
  }

  private void addToQuorumSet(String serverId, PeerHandler ph) {
    this.quorumSet.put(serverId, ph);
    notifyQuorumSetChange();
  }

  private void removeFromQuorumSet(String serverId) {
    this.quorumSet.remove(serverId);
    notifyQuorumSetChange();
  }
}
