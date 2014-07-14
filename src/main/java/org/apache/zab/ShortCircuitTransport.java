/**
 * Licensed to the Apache Software Foundatlion (ASF) under one
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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import org.apache.zab.Participant.MessageTuple;
import org.apache.zab.proto.ZabMessage.Message;
import org.apache.zab.proto.ZabMessage.Message.MessageType;
import org.apache.zab.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Short circuit transport which is used by PeerHandler of leader to put
 * the request into local processor instead of sending to remote followers.
 */
class ShortCircuitTransport extends Transport {

  private static final Logger LOG =
      LoggerFactory.getLogger(ShortCircuitTransport.class);

  final SyncProposalProcessor syncProcessor;

  final CommitProcessor commitProcessor;

  final BlockingQueue<MessageTuple> messageQueue;

  public ShortCircuitTransport(SyncProposalProcessor syncProcessor,
                               CommitProcessor commitProcessor,
                               BlockingQueue<MessageTuple> messageQueue) {
    super(null);
    this.syncProcessor = syncProcessor;
    this.commitProcessor = commitProcessor;
    this.messageQueue = messageQueue;
  }

  @Override
  public void send(String destination, ByteBuffer message) {

    try {
      // Parses it to protocol message.
      byte[] buffer = new byte[message.remaining()];
      message.get(buffer);
      Message msg = Message.parseFrom(buffer);

      if (msg.getType() == MessageType.PROPOSAL) {
        // Got PROPOSAL message from itself, accept it locally.
        Request req = new Request(destination, msg);
        syncProcessor.processRequest(req);
        commitProcessor.processRequest(req);
      } else if (msg.getType() == MessageType.COMMIT) {
        // Got COMMIT message from itself, deliver it locally.
        commitProcessor.processRequest(new Request(destination, msg));
      } else if (msg.getType() == MessageType.HEARTBEAT) {
        Message heartbeat = MessageBuilder.buildHeartbeat();
        MessageTuple tuple = new MessageTuple(destination, heartbeat);
        this.messageQueue.add(tuple);
      }
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Exception when parse protocol buffer.", e);
    }
  }

  @Override
  public void broadcast(Iterator<String> peers, ByteBuffer message) {
    // Does nothing.
  }

  @Override
  public void clear(String destination) {
    // Does nothing.
  }

  @Override
  public void shutdown() {
    // Does nothing.
  }
}
