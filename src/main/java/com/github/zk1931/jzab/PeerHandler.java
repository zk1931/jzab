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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import com.github.zk1931.jzab.proto.ZabMessage.Message;
import com.github.zk1931.jzab.proto.ZabMessage.Message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles peer connection.
 */
class PeerHandler {
  /**
   * Last proposed epoch of the follower, leader needs to know the last proposed
   * proposed of each follower in order to propose a new epoch.
   */
  protected long lastProposedEpoch = -1;

  /**
   * Last acknowledged epoch of the follower, it helps leader decides whose
   * transaction history will be selected as initial history.
   */
  protected long lastAckedEpoch = -1;

  /**
   * Last replied HEARTBEAT time of the peer (in nanoseconds).
   */
  protected long lastHeartbeatTimeNs;

  /**
   * Last transaction id in peer's log or snapshot. It's used by leader to
   * decide how to synchronize the follower in synchronizing phase.
   */
  protected Zxid lastZxid = null;

  /**
   * The last acknowledged transaction id from this peer. The CommitProcessor
   * will query the last acknowledged zxid of each peer and decide which
   * transaction can be committed.
   */
  protected Zxid lastAckedZxid = null;

  /**
   * The server id of the peer.
   */
  protected final String serverId;

  /**
   * The broadcasting queue of peer.
   */
  protected final BlockingQueue<Message> broadcastingQueue =
      new LinkedBlockingQueue<Message>();

  /**
   * The synchronizing task. It's used for synchronizing the followers.
   */
  protected Participant.SyncPeerTask syncTask = null;

  /**
   * The epoch in the NEWLEADER message. It will be sent to followers after the
   * synchronization is done.
   */
  protected long newleaderEpoch;

  /**
   * The transport of the leader, used to send messages to peers.
   */
  protected final Transport transport;

  /**
   * The intervals of sending HEARTBEAT messages.
   */
  protected final int heartbeatIntervalMs;

  /**
   * It's used to run synchronization task and broadcasting task.
   */
  private ExecutorService es = null;

  /**
   * Whether the PeerHandler is allowed to send out messages. Leader will
   * disable the abilities of sending message once it catches the DISCONNECT
   * message from the peer in broadcasting phase so that the messages enqueued
   * by PreProcessor/CommitProcessor will not be sent via transport.
   */
  volatile boolean disableSending = false;

  /**
   * The future of synchronizing task.
   */
  protected Future<Void> ftSync = null;

  /**
   * The future of broadcasting task.
   */
  protected Future<Void> ftBroad = null;

  /**
   * If this peer needs to be synchronized, this records the zxid of last
   * transaction in the synchronization.
   */
  private Zxid lastSyncedZxid = null;

  /**
   * The zxid of the last request of the peer.
   */
  private Zxid lastProposedZxid = Zxid.ZXID_NOT_EXIST;

  /**
   * The timeout for synchronization.
   */
  private int syncTimeoutMs = 0;

  /**
   * The zxid of last COMMIT message sent to the peer. The CommitProcessr uses
   * this to avoid sending duplicate COMMIT messages to the same peer.
   */
  private Zxid lastCommittedZxid = Zxid.ZXID_NOT_EXIST;

  /**
   * If the peer is disconnected.
   */
  private boolean disconnected = false;

  private static final Logger LOG = LoggerFactory.getLogger(PeerHandler.class);

  /**
   * Constructs PeerHandler object.
   *
   * @param serverId the server id of the peer.
   * @param transport the transport object used to send messages.
   * @param heartbeatIntervalMs the interval of sending HEARTBEAT messages.
   */
  public PeerHandler(String serverId,
                     Transport transport,
                     int heartbeatIntervalMs) {
    this.serverId = serverId;
    this.transport = transport;
    this.heartbeatIntervalMs = heartbeatIntervalMs;
    this.es = Executors.newSingleThreadExecutor();
    updateHeartbeatTime();
  }

  Zxid getLastSyncedZxid() {
    return this.lastSyncedZxid;
  }

  void setLastSyncedZxid(Zxid zxid) {
    this.lastSyncedZxid = zxid;
  }

  String getServerId() {
    return this.serverId;
  }

  long getLastHeartbeatTime() {
    return this.lastHeartbeatTimeNs;
  }

  void updateHeartbeatTime() {
    this.lastHeartbeatTimeNs = System.nanoTime();
  }

  long getLastProposedEpoch() {
    return this.lastProposedEpoch;
  }

  void setLastProposedEpoch(long epoch) {
    this.lastProposedEpoch = epoch;
  }

  long getLastAckedEpoch() {
    return this.lastAckedEpoch;
  }

  void setLastAckedEpoch(long epoch) {
    this.lastAckedEpoch = epoch;
  }

  Zxid getLastZxid() {
    return this.lastZxid;
  }

  void setLastZxid(Zxid zxid) {
    this.lastZxid = zxid;
  }

  void setLastAckedZxid(Zxid zxid) {
    this.lastAckedZxid = zxid;
  }

  Zxid getLastAckedZxid() {
    return this.lastAckedZxid;
  }

  void setLastCommittedZxid(Zxid zxid) {
    this.lastCommittedZxid = zxid;
  }

  Zxid getLastCommittedZxid() {
    return this.lastCommittedZxid;
  }

  Zxid getLastProposedZxid() {
    return this.lastProposedZxid;
  }

  void setLastProposedZxid(Zxid zxid) {
    this.lastProposedZxid = zxid;
  }

  synchronized void disableSending() {
    this.disableSending = true;
  }

  boolean isSyncStarted() {
    return this.ftSync != null;
  }

  boolean isSynchronizing() {
    return this.ftSync != null && this.ftBroad == null;
  }

  void setSyncTimeoutMs(int timeout) {
    this.syncTimeoutMs = timeout;
  }

  int getSyncTimeoutMs() {
    return this.syncTimeoutMs;
  }

  void markDisconnected() {
    this.disconnected = true;
  }

  boolean isDisconnected() {
    return this.disconnected;
  }

  /**
   * Puts message in queue.
   *
   * @param msg the message which will be sent.
   */
  void queueMessage(Message msg) {
    this.broadcastingQueue.add(msg);
  }

  void setSyncTask(Participant.SyncPeerTask task, long newLeaderEpoch) {
    this.syncTask = task;
    this.newleaderEpoch = newLeaderEpoch;
  }

  synchronized void sendMessage(Message msg) {
    if (!this.disableSending) {
      this.transport.send(this.serverId, msg);
    }
  }

  void shutdown() throws InterruptedException, ExecutionException {
    if (this.ftSync != null) {
      try {
        // We can't rely on future.cancel() to terminate thread.
        this.syncTask.stop();
        this.ftSync.get();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    if (this.ftBroad != null) {
      Message shutdown = MessageBuilder.buildShutDown();
      this.broadcastingQueue.add(shutdown);
      this.ftBroad.get();
    }
    this.transport.clear(this.serverId);
    LOG.debug("PeerHandler of {} has been shut down {}.", this.serverId);
    this.es.shutdown();
  }

  void startSynchronizingTask() {
    if (this.syncTask == null) {
      throw new RuntimeException("SyncTask is null!");
    }
    if (this.ftSync != null) {
      throw new RuntimeException("SyncTask can be only started once!");
    }
    if (this.ftBroad != null) {
      throw new RuntimeException("SyncTask must be started before "
          + "broadcasting task");
    }
    this.ftSync = this.es.submit(new SyncFollower());
  }

  void startBroadcastingTask() {
    if (this.ftBroad != null) {
      throw new RuntimeException("Broadcast task can be only started once!");
    }
    this.ftBroad = this.es.submit(new BroadcastingFollower());
  }

  class SyncFollower implements Callable<Void> {
    @Override
    public Void call() throws IOException {
      LOG.debug("SyncFollower task to {} task gets started.", serverId);
      // First synchronize the follower state.
      syncTask.run();
      // Send NEW_LEADER message to the follower.
      Message nl = MessageBuilder.buildNewLeader(newleaderEpoch);
      sendMessage(nl);
      return null;
    }
  }

  class BroadcastingFollower implements Callable<Void> {
    @Override
    public Void call() throws InterruptedException {
      LOG.debug("BroadcastingFollower to {} task gets started.", serverId);
      Message heartbeat = MessageBuilder.buildHeartbeat();
      while (true) {
        Message msg = broadcastingQueue.poll(heartbeatIntervalMs,
                                             TimeUnit.MILLISECONDS);
        if (msg == null) {
          // Only send HEARTBEAT message if there hasn't been any other outgoing
          // messages for a certain duration.
          sendMessage(heartbeat);
          continue;
        }
        if (msg.getType() == MessageType.SHUT_DOWN) {
          // shutdown method is called.
          return null;
        }
        if (msg.getType() == MessageType.PROPOSAL) {
          // Got PROPOSAL message, send it to follower.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received PROPOSAL {}",
                      TextFormat.shortDebugString(msg));
          }
          // Sends this PROPOSAL if it's not the duplicate.
          sendMessage(msg);
        } else if (msg.getType() == MessageType.COMMIT) {
          // Got COMMIT message, send it to follower.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received COMMIT {}",
                      TextFormat.shortDebugString(msg));
          }
          // Sends this COMMIT if it's not the duplicate.
          sendMessage(msg);
        } else {
          // Got FLUSH message.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received msg {}", TextFormat.shortDebugString(msg));
          }
          sendMessage(msg);
        }
      }
    }
  }
}
