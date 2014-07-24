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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.zab.proto.ZabMessage.Message;
import org.apache.zab.proto.ZabMessage.Message.MessageType;
import org.apache.zab.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles peer connection.
 */
public class PeerHandler {

  /**
   * Last proposed epoch of the follower, it helps leader decides new
   * proposed epoch.
   */
  protected int lastProposedEpoch = -1;

  /**
   * Last acknowledged epoch of the follower, it helps leader decides whose
   * transaction history will be selected as initial history.
   */
  protected int lastAckedEpoch = -1;

  /**
   * Last replied HEARTBEAT time of the peer (in nanoseconds).
   */
  protected long lastHeartbeatTime;

  /**
   * Last transaction id in peer's log. It's used by leader to decide how to
   * synchronize the follower in synchronizing phase.
   */
  protected Zxid lastZxid = null;

  /**
   * The last acknowledged transaction id from this peer. It's used by
   * CommitProcessor to find out the transaction which will be committed.
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
   * The synchronizing task. For the handler of leader, it's null.
   */
  protected Participant.SyncPeerTask syncTask = null;

  /**
   * The epoch of the NEWLEADER message. It will be sent to follower after the
   * synchronization is done.
   */
  protected int newleaderEpoch;

  /**
   * Passed by Participant to send messages.
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
   * Whether the PeerHandler is allowed to send out message.
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

  String getServerId() {
    return this.serverId;
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

  synchronized void disableSending() {
    this.disableSending = true;
  }

  /**
   * Puts message in queue.
   *
   * @param msg the message which will be sent.
   */
  void queueMessage(Message msg) {
    this.broadcastingQueue.add(msg);
  }

  void setSyncTask(Participant.SyncPeerTask task, int newLeaderEpoch) {
    this.syncTask = task;
    this.newleaderEpoch = newLeaderEpoch;
  }

  synchronized void sendMessage(Message msg) {
    if (!this.disableSending) {
      this.transport.send(this.serverId, ByteBuffer.wrap(msg.toByteArray()));
    }
  }

  void shutdown() {
    if (this.ftSync != null) {
      this.ftSync.cancel(true);
    }
    if (this.ftBroad != null) {
      this.ftBroad.cancel(true);
    }
    this.transport.clear(this.serverId);
    LOG.debug("PeerHandler of {} has been shut down.", this.serverId);
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
