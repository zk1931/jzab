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

package org.apache.zab.transport;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.HashMap;
import org.apache.zab.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mock implementation of transport interface. Used for test purpose.
 */
public class DummyTransport extends Transport {
  private static final Logger LOG = LoggerFactory
                                    .getLogger(DummyTransport.class);

  // Maps server id to its incoming message queue.
  private static ConcurrentHashMap<String, BlockingQueue<Message>> queueMap;
  private final String serverId;
  private BlockingQueue<Message> incomingQueue;

  static {
    queueMap = new ConcurrentHashMap<String, BlockingQueue<Message>>();
  }

  /**
   *  Constructs a dummy transport object.
   *
   * @param serverId the id of this transport
   * @param receiver the receiver of that needs to be notified when
   * the transport receives messages.
   */
  public DummyTransport(String serverId, Receiver receiver) {
    super(receiver);
    this.serverId = serverId;
    this.receiver = receiver;
    initIncomingQueue();
    // Starts receiving thread.
    Executors.newSingleThreadExecutor(DaemonThreadFactory.FACTORY).execute(
        new ReceivingThread());
  }

  /**
   * Initializes the incoming message queue.
   */
  private void initIncomingQueue() {
    this.incomingQueue = queueMap
                         .putIfAbsent(this.serverId,
                                      new LinkedBlockingQueue<Message>());
    if (this.incomingQueue == null) {
      this.incomingQueue = queueMap.get(this.serverId);
    }
  }

  /**
   * Sends a message to a specific server. The channel delivers
   * the message in FIFO order. In the dummy transport, it simply
   * puts the message in the receiving queue of peers.
   *
   * @param destination the id of the message destination
   * @param message the message to be sent
   */
  @Override
  public void send(String destination, ByteBuffer message) {
    send(destination, new Message(this.serverId, message));
  }

  /**
   * Sends a message represents delays.
   *
   * @param destination the id of the message destination
   * @param delayMs the delays in milliseconds
   */
  public void addDelayMs(String destination, int delayMs) {
    send(destination, new DelayMessage(this.serverId, delayMs));
  }

  /**
   * Sends a message represents disconnection.
   *
   * @param destination the id of the message destination.
   */
  @Override
  public void disconnect(String destination) {
    send(destination, new DisconnectMessage(this.serverId));
  }

  protected void send(String destination, Message msg) {
    BlockingQueue<Message> destQueue = null;
    if (queueMap.containsKey(destination)) {
      destQueue = queueMap.get(destination);
    } else {
      destQueue = queueMap.putIfAbsent(destination,
                                       new LinkedBlockingQueue<Message>());
      if (destQueue == null) {
        destQueue = queueMap.get(destination);
      }
    }
    destQueue.add(msg);
  }

  public static void clearMessageQueue() {
    queueMap.clear();
  }

  /**
   * Thread that is responsible for receiving messages.
   */
  private class ReceivingThread implements Runnable {
    // For receiver, one sender per thread.
    AbstractMap<String, ExecutorService> recvTasks =
        new HashMap<String, ExecutorService>();

    @Override
    public void run() {
      Message msg = null;
      while (true) {
        try {
          msg = incomingQueue.take();
          String source = msg.getServer();
          // Check if we have a receiving thread for this sender.
          if (!recvTasks.containsKey(source)) {
            // If no, create one for the sender.
            recvTasks.put(source,
                          Executors
                          .newSingleThreadExecutor(DaemonThreadFactory
                                                   .FACTORY));
          }
          // Put the delivered message to its notification thread queue.
          recvTasks.get(source).execute(new NotifyTask(msg));

        } catch (InterruptedException e) {
          LOG.error("Interrputed exception in recving message thread.", e);
        }
      }
    }

    /**
     * Used to notify the receiver of one received message.
     */
    class NotifyTask implements Runnable {
      Message msg;

      public NotifyTask(Message msg) {
        this.msg = msg;
      }

      @Override
      public void run() {
        if (this.msg instanceof DelayMessage) {
          // It is the message represents the delay.
          try {
            // Simulate the delay by sleeping for a while.
            Thread.sleep(((DelayMessage)this.msg).getDelayTimeMs());
          } catch (InterruptedException e) {
            LOG.error("Caught interrupt exception when performing delay.", e);
          }
        } else if (this.msg instanceof DisconnectMessage) {
          receiver.onDisconnected(this.msg.getServer());
        } else {
          // Notify the receiver.
          receiver.onReceived(this.msg.getServer(),
                              this.msg.getBody().duplicate());
        }
      }
    }
  }

  /**
   * Message used for communication.
   */
  static class Message {
    private final ByteBuffer body;
    private final String server;

    /**
     * Constructs a message object.
     *
     * @param server destination for outgoing messages,
     * source for incoming.
     * @param body the content of the message
     */
    public Message(String server, ByteBuffer body) {
      this.server = server;
      this.body = body;
    }

    /**
     * Get the body of the message.
     *
     * @return the content of the message
     */
    public ByteBuffer getBody() {
      return this.body;
    }

    /**
     * Gets the server id of the message, destination for
     * outgoing messages, source for incoming.
     *
     * @return the string represents the id of the server
     */
    public String getServer() {
      return this.server;
    }
  }

  /**
   * Delayed message. Used to simulate the delay of the message transmission.
   */
  static class DelayMessage extends Message {
    private final int delayTimeMs;

    /**
     * Constructs a message represents delays only. Once the server
     * receives this message, it will simply sleep for delayTime milliseconds.
     *
     * @param server the destination of the message.
     * @param delayTime the how long the sleep should perform.(in milliseconds)
     */
    public DelayMessage(String server, int delayTimeMs) {
      super(server, null);
      this.delayTimeMs = delayTimeMs;
    }

    /**
     * Gets the number of milliseconds need to be performed.
     *
     * @return the milliseconds.
     */
    public int getDelayTimeMs() {
      return this.delayTimeMs;
    }
  }

  /**
   * Message represents disconnection.
   */
  static class DisconnectMessage extends Message {
    public DisconnectMessage(String server) {
      super(server, null);
    }
  }
}
