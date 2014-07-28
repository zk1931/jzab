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

import java.nio.ByteBuffer;
import com.google.protobuf.ByteString;
import org.apache.zab.proto.ZabMessage;
import org.apache.zab.proto.ZabMessage.Ack;
import org.apache.zab.proto.ZabMessage.AckEpoch;
import org.apache.zab.proto.ZabMessage.AddFollower;
import org.apache.zab.proto.ZabMessage.Commit;
import org.apache.zab.proto.ZabMessage.Diff;
import org.apache.zab.proto.ZabMessage.Disconnected;
import org.apache.zab.proto.ZabMessage.FlushPreProcessor;
import org.apache.zab.proto.ZabMessage.FlushSyncProcessor;
import org.apache.zab.proto.ZabMessage.Handshake;
import org.apache.zab.proto.ZabMessage.InvalidMessage;
import org.apache.zab.proto.ZabMessage.Message;
import org.apache.zab.proto.ZabMessage.NewEpoch;
import org.apache.zab.proto.ZabMessage.NewLeader;
import org.apache.zab.proto.ZabMessage.Proposal;
import org.apache.zab.proto.ZabMessage.ProposedEpoch;
import org.apache.zab.proto.ZabMessage.PullTxnReq;
import org.apache.zab.proto.ZabMessage.QueryReply;
import org.apache.zab.proto.ZabMessage.RemoveFollower;
import org.apache.zab.proto.ZabMessage.Request;
import org.apache.zab.proto.ZabMessage.Snapshot;
import org.apache.zab.proto.ZabMessage.Truncate;
import static org.apache.zab.proto.ZabMessage.Message.MessageType;

/**
 * Helper class used for creating protobuf messages.
 */
public final class MessageBuilder {

  private MessageBuilder() {
    // Can't be instantiated.
  }


  /**
   * Converts Zxid object to protobuf Zxid object.
   *
   * @param zxid the Zxid object.
   * @return the ZabMessage.Zxid
   */
  public static ZabMessage.Zxid toProtoZxid(Zxid zxid) {
    ZabMessage.Zxid z = ZabMessage.Zxid.newBuilder()
                                  .setEpoch(zxid.getEpoch())
                                  .setXid(zxid.getXid())
                                  .build();
    return z;
  }

  /**
   * Converts protobuf Zxid object to Zxid object.
   *
   * @param zxid the protobuf Zxid object.
   * @return the Zxid object.
   */
  public static Zxid fromProtoZxid(ZabMessage.Zxid zxid) {
    return new Zxid(zxid.getEpoch(), zxid.getXid());
  }

  /**
   * Converts protobuf Proposal object to Transaction object.
   *
   * @param prop the protobuf Proposal object.
   * @return the Transaction object.
   */
  public static Transaction fromProposal(Proposal prop) {
    Zxid zxid = fromProtoZxid(prop.getZxid());
    ByteBuffer buffer = prop.getBody().asReadOnlyByteBuffer();
    return new Transaction(zxid, buffer);
  }

  /**
   * Converts Transaction object to protobuf Proposal object.
   *
   * @param txn the Transaction object.
   * @return the protobuf Proposal object.
   */
  public static Proposal fromTransaction(Transaction txn) {
    ZabMessage.Zxid zxid = toProtoZxid(txn.getZxid());
    ByteString bs = ByteString.copyFrom(txn.getBody());
    return Proposal.newBuilder().setZxid(zxid).setBody(bs).build();
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
    ZabMessage.Zxid zxid = toProtoZxid(lastZxid);

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
    ZabMessage.Zxid zxid = toProtoZxid(lastZxid);

    PullTxnReq req = PullTxnReq.newBuilder().setLastZxid(zxid)
                                            .build();

    return Message.newBuilder().setType(MessageType.PULL_TXN_REQ)
                               .setPullTxnReq(req)
                               .build();
  }

  /**
   * Creates a PROPOSAL message.
   *
   * @param txn the transaction of this proposal.
   * @return a protobuf message.
   */
  public static Message buildProposal(Transaction txn) {
    ZabMessage.Zxid zxid = toProtoZxid(txn.getZxid());

    Proposal prop = Proposal.newBuilder()
                            .setZxid(zxid)
                            .setBody(ByteString.copyFrom(txn.getBody()))
                            .build();

    return Message.newBuilder().setType(MessageType.PROPOSAL)
                               .setProposal(prop)
                               .build();
  }

  /**
   * Creates a PROPOSAL message.
   *
   * @param txn the transaction of this proposal.
   * @param clientId the id of the client who sends the request.
   * @return a protobuf message.
   */
  public static Message buildProposal(Transaction txn, String clientId) {
    ZabMessage.Zxid zxid = toProtoZxid(txn.getZxid());
    Proposal prop = Proposal.newBuilder()
                            .setZxid(zxid)
                            .setBody(ByteString.copyFrom(txn.getBody()))
                            .setClientId(clientId)
                            .build();

    return Message.newBuilder().setType(MessageType.PROPOSAL)
                               .setProposal(prop)
                               .build();
  }

  /**
   * Creates a DIFF message.
   *
   * @param lastZxid the last zxid of the server who initiates the sync.
   * @return a protobuf message.
   */
  public static Message buildDiff(Zxid lastZxid) {
    ZabMessage.Zxid lz = toProtoZxid(lastZxid);

    Diff diff = Diff.newBuilder().setLastZxid(lz).build();

    return Message.newBuilder().setType(MessageType.DIFF)
                               .setDiff(diff)
                               .build();
  }

  /**
   * Creates a TRUNCATE  message.
   *
   * @param lastPrefixZxid truncate receiver's log from lastPrefixZxid.
   * (not including it)
   * @return a protobuf message.
   */
  public static Message buildTruncate(Zxid lastPrefixZxid) {
    ZabMessage.Zxid lpz = toProtoZxid(lastPrefixZxid);

    Truncate trunc = Truncate.newBuilder().setLastPrefixZxid(lpz)
                                          .build();

    return Message.newBuilder().setType(MessageType.TRUNCATE)
                               .setTruncate(trunc)
                               .build();

  }

  /**
   * Creates a NEW_LEADER message.
   *
   * @param epoch the established epoch.
   * @return a protobuf message.
   */
  public static Message buildNewLeader(int epoch) {
    NewLeader nl = NewLeader.newBuilder().setEpoch(epoch).build();
    return Message.newBuilder().setType(MessageType.NEW_LEADER)
                               .setNewLeader(nl)
                               .build();
  }

  /**
   * Creates a ACK message.
   *
   * @param zxid the zxid of the transaction ACK.
   * @return a protobuf message.
   */
  public static Message buildAck(Zxid zxid) {
    ZabMessage.Zxid zzxid = toProtoZxid(zxid);
    Ack ack = Ack.newBuilder().setZxid(zzxid).build();
    return Message.newBuilder().setType(MessageType.ACK).setAck(ack).build();
  }

  /**
   * Creates a SNAPSHOT message.
   *
   * @param lastZxid the last zxid of the sender.
   * @return a protobuf message.
   */
  public static Message buildSnapshot(Zxid lastZxid) {
    ZabMessage.Zxid zxid = toProtoZxid(lastZxid);
    Snapshot snapshot = Snapshot.newBuilder().setLastZxid(zxid).build();

    return Message.newBuilder().setType(MessageType.SNAPSHOT)
                               .setSnapshot(snapshot)
                               .build();
  }

  /**
   * Creates a REQUEST message.
   *
   * @param request the ByteBuffer represents the request.
   * @return a protobuf message.
   */
  public static Message buildRequest(ByteBuffer request) {
    Request req = Request.newBuilder()
                         .setRequest(ByteString.copyFrom(request))
                         .build();

    return Message.newBuilder().setType(MessageType.REQUEST)
                               .setRequest(req)
                               .build();
  }

  /**
   * Creates a COMMIT message.
   *
   * @param zxid the id of the committed transaction.
   * @return a protobuf message.
   */
  public static Message buildCommit(Zxid zxid) {
    ZabMessage.Zxid cZxid = toProtoZxid(zxid);

    Commit commit = Commit.newBuilder().setZxid(cZxid).build();

    return Message.newBuilder().setType(MessageType.COMMIT)
                               .setCommit(commit)
                               .build();
  }

  /**
   * Creates a HANDSHAKE message.
   *
   * @param nodeId the node ID in the handshake message.
   * @return a protobuf message.
   */
  public static Message buildHandshake(String nodeId) {
    Handshake handshake = Handshake.newBuilder().setNodeId(nodeId).build();
    return Message.newBuilder().setType(MessageType.HANDSHAKE)
                               .setHandshake(handshake)
                               .build();
  }

  /**
   * Creates a HEARTBEAT message.
   *
   * @return a protobuf message.
   */
  public static Message buildHeartbeat() {
    return Message.newBuilder().setType(MessageType.HEARTBEAT).build();
  }

  /**
   * Creates a FLUSH_PREPROCESSOR message.
   *
   * @param followerId the ID of new joined follower.
   * @return a protobuf message.
   */
  public static Message buildFlushPreProcessor(String followerId) {
    FlushPreProcessor flush = FlushPreProcessor.newBuilder()
                                               .setFollowerId(followerId)
                                               .build();
    return Message.newBuilder().setType(MessageType.FLUSH_PREPROCESSOR)
                               .setFlushPreProcessor(flush)
                               .build();
  }

  /**
   * Creates a FLUSH_SYNCPROCESSOR message.
   *
   * @param followerId the ID of new joined follower.
   * @return a protobuf message.
   */
  public static Message buildFlushSyncProcessor(String followerId) {
    return buildFlushSyncProcessor(followerId, Zxid.ZXID_NOT_EXIST);
  }

  /**
   * Creates a FLUSH_SYNCPROCESSOR message.
   *
   * @param followerId the ID of new joined follower.
   * @param zxid the last appended zxid of SyncProposalProcessor.
   * @return a protobuf message.
   */
  public static Message buildFlushSyncProcessor(String followerId, Zxid zxid) {
    ZabMessage.Zxid pZxid = toProtoZxid(zxid);
    FlushSyncProcessor flush = FlushSyncProcessor.newBuilder()
                                                 .setFollowerId(followerId)
                                                 .setLastAppendedZxid(pZxid)
                                                 .build();
    return Message.newBuilder().setType(MessageType.FLUSH_SYNCPROCESSOR)
                               .setFlushSyncProcessor(flush)
                               .build();
  }

  /**
   * Creates a ADD_FOLLOWER message.
   *
   * @param followerId the ID of new joined follower.
   * @return a protobuf message.
   */
  public static Message buildAddFollower(String followerId) {
    AddFollower addFollower = AddFollower.newBuilder()
                                         .setFollowerId(followerId)
                                         .build();
    return Message.newBuilder().setType(MessageType.ADD_FOLLOWER)
                               .setAddFollower(addFollower)
                               .build();
  }

  /**
   * Creates a DISCONNECTED message.
   *
   * @param serverId the ID of the disconnected peer.
   * @return a protobuf message.
   */
  public static Message buildDisconnected(String serverId) {
    Disconnected dis = Disconnected.newBuilder()
                                   .setServerId(serverId)
                                   .build();
    return Message.newBuilder().setType(MessageType.DISCONNECTED)
                               .setDisconnected(dis)
                               .build();
  }

  /**
   * Creates a REMOVE_FOLLOWER message.
   *
   * @param followerId the ID of disconnected follower.
   * @return a protobuf message.
   */
  public static Message buildRemoveFollower(String followerId) {
    RemoveFollower removeFollower = RemoveFollower.newBuilder()
                                                  .setFollowerId(followerId)
                                                  .build();
    return Message.newBuilder().setType(MessageType.REMOVE_FOLLOWER)
                               .setRemoveFollower(removeFollower)
                               .build();
  }

  public static Message buildQueryLeader() {
    return Message.newBuilder().setType(MessageType.QUERY_LEADER).build();
  }

  public static Message buildQueryReply(String leader) {
    QueryReply reply = QueryReply.newBuilder()
                                 .setLeader(leader)
                                 .build();
    return Message.newBuilder().setType(MessageType.QUERY_REPLY)
                               .setReply(reply)
                               .build();
  }

  public static Message buildJoin() {
    return Message.newBuilder().setType(MessageType.JOIN).build();
  }

  public static Message buildCop(Configuration config) {
    ZabMessage.Zxid version = toProtoZxid(config.getVersion());
    ZabMessage.Configuration zConfig = ZabMessage.Configuration.newBuilder()
                                        .setVersion(version)
                                        .addAllServers(config.getPeers())
                                        .build();
    return Message.newBuilder().setType(MessageType.COP)
                               .setCop(zConfig)
                               .build();
  }
}
