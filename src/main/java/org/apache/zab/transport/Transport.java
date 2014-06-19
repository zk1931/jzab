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

/**
 * Abstract transport class. Used for communication between different
 * Zab instances. It will handle connections to different peers underneath.
 * The transport will also handle reconnections. Before it reconnects to
 * the disconnected peer, it will first call onDisconnected callback of
 * receiver.
 */
public abstract class Transport {

  protected Receiver receiver;

  public Transport(Receiver r) {
    this.receiver = r;
  }

  /**
   * Sends a message to a specific server. The channel delivers
   * the message in FIFO order.
   *
   * @param destination the id of the message destination
   * @param message the message to be sent
   */
  public abstract void send(String destination, ByteBuffer message);

  /**
   * Interface of receiver class. Transport will notify the receiver of
   * arrived messages.
   */
  public interface Receiver {
    /**
     * Callback that will be called by Transport class once the message
     * is arrived. The message is guaranteed to be received in FIFO order,
     * which means if message m1 is sent before m2 from sender s, then m1
     * is guaranteed to be received first.
     *
     * @param source the id of the server who sent the message
     * @param message the message
     */
    void onReceived(String source, ByteBuffer message);

    /**
     * Callback that notifies the the connection to peer is disconnected.
     *
     * @param serverId the id of peer which is disconnected from this one.
     */
    void onDisconnected(String serverId);
  }
}
