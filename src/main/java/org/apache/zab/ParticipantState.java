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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.zab.proto.ZabMessage.Message;
import org.apache.zab.transport.NettyTransport;
import org.apache.zab.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Participant state. It will be passed accross different instances of
 * Leader/Follower class during the lifetime of Zab.
 */
public class ParticipantState implements Transport.Receiver {
  /**
   * Persistent variables for Zab.
   */
  private final PersistentState persistence;

  /**
   * The last delivered zxid. It's not persisted to disk. Just a place holder
   * to pass between Follower/Leader.
   */
  private Zxid lastDeliveredZxid = Zxid.ZXID_NOT_EXIST;

  /**
   * Message queue. The receiving callback simply parses the message and puts
   * it in queue, it's up to Leader/Follower/Election to take out
   * and process the message.
   */
  private final BlockingQueue<MessageTuple> messageQueue =
    new LinkedBlockingQueue<MessageTuple>();

  /**
   * Request queue. This queue buffers all the outgoing requests from clients,
   * they will be processed once Participant enters broadcasting phase.
   */
  private final BlockingQueue<MessageTuple> requestQueue =
    new LinkedBlockingQueue<MessageTuple>();

  /**
   * Used for communication between nodes.
   */
  private final Transport transport;

  /**
   * Server ID of the Participant.
   */
  private final String serverId;

  private static final Logger LOG =
    LoggerFactory.getLogger(ParticipantState.class);

  ParticipantState(PersistentState persistence, String serverId)
      throws InterruptedException, IOException , GeneralSecurityException {
    this.persistence = persistence;
    this.serverId = serverId;
    this.transport = new NettyTransport(this.serverId, this,
                                        persistence.getLogDir());
  }

  @Override
  public void onReceived(String source, Message message) {
    this.messageQueue.add(new MessageTuple(source, message));
  }

  @Override
  public void onDisconnected(String server) {
    LOG.debug("ONDISCONNECTED from {}", server);
    Message disconnected = MessageBuilder.buildDisconnected(server);
    this.messageQueue.add(new MessageTuple(this.serverId,
                                           disconnected));
  }

  public BlockingQueue<MessageTuple> getMessageQueue() {
    return this.messageQueue;
  }

  public BlockingQueue<MessageTuple> getRequestQueue() {
    return this.requestQueue;
  }

  public String getServerId() {
    return this.serverId;
  }

  public Transport getTransport() {
    return this.transport;
  }

  public PersistentState getPersistence() {
    return this.persistence;
  }

  public Zxid getLastDeliveredZxid() {
    return this.lastDeliveredZxid;
  }

  public void updateLastDeliveredZxid(Zxid zxid) {
    this.lastDeliveredZxid = zxid;
  }

  public void enqueueRequest(ByteBuffer buffer) {
    Message msg = MessageBuilder.buildRequest(buffer);
    this.requestQueue.add(new MessageTuple(this.serverId, msg));
  }

  public void enqueueRemove(String peerId) {
    Message msg = MessageBuilder.buildRemove(peerId);
    this.requestQueue.add(new MessageTuple(this.serverId, msg));
  }
}

