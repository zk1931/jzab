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

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import org.apache.zab.proto.ZabMessage;
import org.apache.zab.proto.ZabMessage.AckEpoch;
import org.apache.zab.proto.ZabMessage.InvalidMessage;
import org.apache.zab.proto.ZabMessage.Message;
import org.apache.zab.proto.ZabMessage.NewEpoch;
import org.apache.zab.proto.ZabMessage.Proposal;
import org.apache.zab.proto.ZabMessage.ProposedEpoch;
import org.apache.zab.proto.ZabMessage.PullTxnReq;

import static org.apache.zab.proto.ZabMessage.Message.MessageType;

/**
 * Helper class used for creating protobuf messages.
 */
public final class MessageBuilder {

  private MessageBuilder() {
    // Can't be instantiated.
  }

  /**
   * Creates CEPOCH message.
   *
   * @param epoch the last proposed epoch.
   * @return the protobuf message.
   */
  public static Message buildProposedEpoch(int epoch) {
    ProposedEpoch pEpoch = ProposedEpoch.newBuilder().setProposedEpoch(epoch)
                           .build();

    return Message.newBuilder().setType(MessageType.PROPOSED_EPOCH)
                               .setProposedEpoch(pEpoch)
                               .build();
  }

  /**
   * Creates a NEW_EPOCH message.
   *
   * @param epoch the new proposed epoch number.
   * @return the protobuf message.
   */
  public static Message buildNewEpochMessage(int epoch) {
    NewEpoch nEpoch = NewEpoch.newBuilder().setNewEpoch(epoch).build();

    return  Message.newBuilder().setType(MessageType.NEW_EPOCH)
                                .setNewEpoch(nEpoch)
                                .build();
  }

  /**
   * Creates a ACK_EPOCH message.
   *
   * @param epoch the last leader proposal the follower has acknowledged.
   * @param lastZxid the last zxid of the follower.
   * @return the protobuf message.
   */
  public static Message buildAckEpoch(int epoch, Zxid lastZxid) {
    ZabMessage.Zxid zxid = ZabMessage.Zxid.newBuilder()
                                     .setEpoch(lastZxid.getEpoch())
                                     .setXid(lastZxid.getXid())
                                     .build();

    AckEpoch ackEpoch = AckEpoch.newBuilder()
                        .setAcknowledgedEpoch(epoch)
                        .setLastZxid(zxid)
                        .build();

    return Message.newBuilder().setType(MessageType.ACK_EPOCH)
                               .setAckEpoch(ackEpoch)
                               .build();
  }

  /**
   * Creates an INVALID message. This message will not be transmitted among
   * workers. It's created when protobuf fails to parse the byte buffer.
   *
   * @param content the byte array which can't be parsed by protobuf.
   * @return a message represents an invalid message.
   */
  public static Message buildInvalidMessage(byte[] content) {
    InvalidMessage msg = InvalidMessage.newBuilder()
                         .setReceivedBytes(ByteString.copyFrom(content))
                         .build();

    return Message.newBuilder().setType(MessageType.INVALID_MESSAGE)
                               .setInvalid(msg)
                               .build();
  }

  /**
   * Creates a PULL_TXN_REQ message to ask follower sync its history to leader.
   *
   * @param lastZxid the last transaction id of leader, the sync starts one
   * after this transaction (not including this one).
   * @return a protobuf message.
   */
  public static Message buildPullTxnReq(Zxid lastZxid) {
    ZabMessage.Zxid zxid = ZabMessage.Zxid.newBuilder()
                                     .setEpoch(lastZxid.getEpoch())
                                     .setXid(lastZxid.getXid())
                                     .build();

    PullTxnReq req = PullTxnReq.newBuilder().setLastZxid(zxid)
                                            .build();

    return Message.newBuilder().setType(MessageType.PULL_TXN_REQ)
                               .setPullTxnReq(req)
                               .build();
  }

  /**
   * Creates a PROPOSAL message.
   *
   * @param transactionId the transaction id of this proposal.
   * @param body the content of this transaction.
   * @return a protobuf message.
   */
  public static Message buildProposal(Zxid transactionId, ByteBuffer body) {
    ZabMessage.Zxid zxid = ZabMessage.Zxid.newBuilder()
                                     .setEpoch(transactionId.getEpoch())
                                     .setXid(transactionId.getXid())
                                     .build();

    Proposal prop = Proposal.newBuilder().setZxid(zxid)
                                         .setBody(ByteString.copyFrom(body))
                                         .build();

    return Message.newBuilder().setType(MessageType.PROPOSAL)
                               .setProposal(prop)
                               .build();
  }
}
