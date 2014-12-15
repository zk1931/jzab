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

import java.nio.ByteBuffer;
import com.google.protobuf.ByteString;
import com.github.zk1931.jzab.proto.ZabMessage;
import com.github.zk1931.jzab.proto.ZabMessage.Ack;
import com.github.zk1931.jzab.proto.ZabMessage.AckEpoch;
import com.github.zk1931.jzab.proto.ZabMessage.Commit;
import com.github.zk1931.jzab.proto.ZabMessage.Diff;
import com.github.zk1931.jzab.proto.ZabMessage.Disconnected;
import com.github.zk1931.jzab.proto.ZabMessage.Handshake;
import com.github.zk1931.jzab.proto.ZabMessage.InvalidMessage;
import com.github.zk1931.jzab.proto.ZabMessage.Message;
import com.github.zk1931.jzab.proto.ZabMessage.NewEpoch;
import com.github.zk1931.jzab.proto.ZabMessage.NewLeader;
import com.github.zk1931.jzab.proto.ZabMessage.Proposal;
import com.github.zk1931.jzab.proto.ZabMessage.ProposedEpoch;
import com.github.zk1931.jzab.proto.ZabMessage.PullTxnReq;
import com.github.zk1931.jzab.proto.ZabMessage.QueryReply;
import com.github.zk1931.jzab.proto.ZabMessage.Remove;
import com.github.zk1931.jzab.proto.ZabMessage.Request;
import com.github.zk1931.jzab.proto.ZabMessage.Snapshot;
import com.github.zk1931.jzab.proto.ZabMessage.Truncate;
import static com.github.zk1931.jzab.proto.ZabMessage.Message.MessageType;
import static com.github.zk1931.jzab.proto.ZabMessage.Proposal.ProposalType;

/**
 * Helper class used for creating protobuf messages.
 */
final class MessageBuilder {

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
    return new Transaction(zxid, prop.getType().getNumber(), buffer);
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
    return Proposal.newBuilder().setZxid(zxid).setBody(bs)
                   .setType(ProposalType.values()[txn.getType()]).build();
  }

  /**
   * Creates CEPOCH message.
   *
   * @param proposedEpoch the last proposed epoch.
   * @param acknowledgedEpoch the acknowledged epoch.
   * @param config the current seen configuration.
   * @return the protobuf message.
   */
  public static Message buildProposedEpoch(long proposedEpoch,
                                           long acknowledgedEpoch,
                                           ClusterConfiguration config,
                                           int syncTimeoutMs) {
    ZabMessage.Zxid version = toProtoZxid(config.getVersion());
    ZabMessage.ClusterConfiguration zCnf
      = ZabMessage.ClusterConfiguration.newBuilder()
                                       .setVersion(version)
                                       .addAllServers(config.getPeers())
                                       .build();
    ProposedEpoch pEpoch = ProposedEpoch.newBuilder()
                                        .setProposedEpoch(proposedEpoch)
                                        .setCurrentEpoch(acknowledgedEpoch)
                                        .setConfig(zCnf)
                                        .setSyncTimeout(syncTimeoutMs)
                                        .build();
    return Message.newBuilder().setType(MessageType.PROPOSED_EPOCH)
                               .setProposedEpoch(pEpoch)
                               .build();
  }

  /**
   * Creates a NEW_EPOCH message.
   *
   * @param epoch the new proposed epoch number.
   * @param syncTimeoutMs the timeout for synchronization.
   * @return the protobuf message.
   */
  public static Message buildNewEpochMessage(long epoch, int syncTimeoutMs) {
    NewEpoch nEpoch = NewEpoch.newBuilder()
                              .setNewEpoch(epoch)
                              .setSyncTimeout(syncTimeoutMs)
                              .build();
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
  public static Message buildAckEpoch(long epoch, Zxid lastZxid) {
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
                            .setType(ProposalType.values()[txn.getType()])
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
                            .setType(ProposalType.values()[txn.getType()])
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
   * @param lastPrefixZxid truncate receiver's log from lastPrefixZxid
   * exclusively.
   * @param lastZxid the last zxid for the synchronization.
   * @return a protobuf message.
   */
  public static Message buildTruncate(Zxid lastPrefixZxid, Zxid lastZxid) {
    ZabMessage.Zxid lpz = toProtoZxid(lastPrefixZxid);
    ZabMessage.Zxid lz = toProtoZxid(lastZxid);
    Truncate trunc = Truncate.newBuilder().setLastPrefixZxid(lpz)
                                          .setLastZxid(lz)
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
  public static Message buildNewLeader(long epoch) {
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
   * Creates a SNAPSHOT message.
   *
   * @param lastZxid the last zxid of the sender.
   * @param snapZxid the last guaranteed applied zxid in snapshot.
   * @return a protobuf message.
   */
  public static Message buildSnapshot(Zxid lastZxid, Zxid snapZxid) {
    ZabMessage.Zxid lZxid = toProtoZxid(lastZxid);
    ZabMessage.Zxid sZxid = toProtoZxid(snapZxid);
    Snapshot snapshot = Snapshot.newBuilder().setLastZxid(lZxid)
                                             .setSnapZxid(sZxid)
                                             .build();
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
   * Creates a QUERY_LEADER message.
   *
   * @return a protobuf message.
   */
  public static Message buildQueryLeader() {
    return Message.newBuilder().setType(MessageType.QUERY_LEADER).build();
  }

  /**
   * Creates a QUERY_REPLY message.
   *
   * @param leader the current leader in broadcasting phase.
   * @return a protobuf message.
   */
  public static Message buildQueryReply(String leader) {
    QueryReply reply = QueryReply.newBuilder()
                                 .setLeader(leader)
                                 .build();
    return Message.newBuilder().setType(MessageType.QUERY_LEADER_REPLY)
                               .setReply(reply)
                               .build();
  }

  /**
   * Creates a JOIN message.
   *
   * @return a protobuf message.
   */
  public static Message buildJoin(Zxid lastZxid) {
    ZabMessage.Zxid zxid = toProtoZxid(lastZxid);
    ZabMessage.Join join =
      ZabMessage.Join.newBuilder().setLastZxid(zxid).build();
    return Message.newBuilder().setType(MessageType.JOIN).setJoin(join).build();
  }

  /**
   * Creates a ZabMessage.ClusterConfiguration object.
   *
   * @param config the ClusterConfiguration object.
   * @return protobuf ClusterConfiguration object.
   */
  public static ZabMessage.ClusterConfiguration
  buildConfig(ClusterConfiguration config) {
    ZabMessage.Zxid version = toProtoZxid(config.getVersion());
    return ZabMessage.ClusterConfiguration.newBuilder()
                                          .setVersion(version)
                                          .addAllServers(config.getPeers())
                                          .build();
  }

  /**
   * Creates a SYNC_END message.
   *
   * @param config the cluster configuration.
   * @return a protobuf message.
   */
  public static Message buildSyncEnd(ClusterConfiguration config) {
    ZabMessage.Zxid version = toProtoZxid(config.getVersion());
    ZabMessage.ClusterConfiguration zConfig
      = ZabMessage.ClusterConfiguration.newBuilder()
                                       .setVersion(version)
                                       .addAllServers(config.getPeers())
                                       .build();
    return Message.newBuilder().setType(MessageType.SYNC_END)
                               .setConfig(zConfig)
                               .build();
  }

  /**
   * Creates a REMOVE message.
   *
   * @param serverId the id of server who will be removed from the cluster
   * configuration.
   * @return a protobuf message.
   */
  public static Message buildRemove(String serverId) {
    Remove remove = Remove.newBuilder().setServerId(serverId).build();
    return Message.newBuilder().setType(MessageType.REMOVE)
                               .setRemove(remove)
                               .build();
  }

  /**
   * Creates a SHUT_DOWN message.
   *
   * @return a protobuf message.
   */
  public static Message buildShutDown() {
    return Message.newBuilder().setType(MessageType.SHUT_DOWN).build();
  }

  /**
   * Creates a DELIVERED message.
   *
   * @param deliveredZxid the last zxid of delivered transaction.
   * @return a protobuf message.
   */
  public static Message buildDelivered(Zxid deliveredZxid) {
    ZabMessage.Zxid zxid = toProtoZxid(deliveredZxid);
    ZabMessage.Delivered delivered =
      ZabMessage.Delivered.newBuilder().setZxid(zxid).build();
    return Message.newBuilder().setType(MessageType.DELIVERED)
                               .setDelivered(delivered).build();
  }

  /**
   * Creates a FILE_HEADER message.
   *
   * @param length the lenght of the file.
   * @return a protobuf message.
   */
  public static Message buildFileHeader(long length) {
    ZabMessage.FileHeader header = ZabMessage.FileHeader.newBuilder()
                                             .setLength(length).build();
    return Message.newBuilder().setType(MessageType.FILE_HEADER)
                               .setFileHeader(header).build();
  }

  /**
   * Creates a FILE_RECEIVED message.
   *
   * @param fullPath the path of the received file.
   * @return a protobuf message.
   */
  public static Message buildFileReceived(String fullPath) {
    ZabMessage.FileReceived received = ZabMessage.FileReceived
                                                 .newBuilder()
                                                 .setFullPath(fullPath)
                                                 .build();
    return Message.newBuilder().setType(MessageType.FILE_RECEIVED)
                               .setFileReceived(received).build();
  }

  /**
   * Creates a FLUSH_REQ message.
   *
   * @param body the data of the request.
   * @return a protobuf message.
   */
  public static Message buildFlushRequest(ByteBuffer body) {
    ZabMessage.FlushRequest flushReq =
      ZabMessage.FlushRequest.newBuilder().setBody(ByteString.copyFrom(body))
                                          .build();
    return Message.newBuilder().setType(MessageType.FLUSH_REQ)
                               .setFlushRequest(flushReq)
                               .build();
  }

  /**
   * Creates a FLUSH message.
   *
   * @param lastProposedZxid the last proposed zxid before the FLUSH.
   * @param body the data of the request.
   * @return a protobuf message.
   */
  public static Message buildFlush(Zxid lastProposedZxid, ByteBuffer body) {
    ZabMessage.Zxid zxid = toProtoZxid(lastProposedZxid);
    ZabMessage.Flush flush = ZabMessage.Flush.newBuilder()
                                             .setZxid(zxid)
                                             .setBody(ByteString.copyFrom(body))
                                             .build();
    return Message.newBuilder().setType(MessageType.FLUSH).setFlush(flush)
                  .build();
  }

  /**
   * Creates a SYNC_HISTORY message. Leader will synchronize everything it has
   * to follower after receiving this message.
   */
  public static Message buildSyncHistory(Zxid lastZxid) {
    ZabMessage.Zxid zxid = toProtoZxid(lastZxid);
    ZabMessage.SyncHistory sync =
      ZabMessage.SyncHistory.newBuilder().setLastZxid(zxid).build();
    return Message.newBuilder().setType(MessageType.SYNC_HISTORY)
                               .setSyncHistory(sync).build();
  }

  /**
   * Creates the SYNC_HISTORY_REPLY message. It's the first time to tell the
   * joiner the synchronization timeout.
   *
   * @param timeout timeout in milliseconds.
   * @return a protobuf message.
   */
  public static Message buildSyncHistoryReply(int timeout) {
    ZabMessage.SyncHistoryReply reply =
      ZabMessage.SyncHistoryReply.newBuilder().setSyncTimeout(timeout).build();
    return Message.newBuilder().setType(MessageType.SYNC_HISTORY_REPLY)
                               .setSyncHistoryReply(reply).build();
  }

  /**
   * Creates the ELECTION_INFO message.
   *
   * @param vote the server who is selected as the leader.
   * @param lastZxid the last zxid of the selected leader.
   * @param round the round number.
   * @param electing true if current server is in electing phase, false otw.
   * @return a protobuf message.
   */
  public static Message buildElectionInfo(String vote, Zxid lastZxid,
                                          long ackEpoch,
                                          long round,
                                          boolean electing) {
    ZabMessage.Zxid zxid = toProtoZxid(lastZxid);
    ZabMessage.ElectionInfo ei = ZabMessage.ElectionInfo.newBuilder()
                                                        .setVote(vote)
                                                        .setZxid(zxid)
                                                        .setAckEpoch(ackEpoch)
                                                        .setIsElecting(electing)
                                                        .setRound(round)
                                                        .build();
    return Message.newBuilder().setType(MessageType.ELECTION_INFO)
                               .setElectionInfo(ei)
                               .build();
  }

  /**
   * Creates the SNAPSHOT_DONE message.
   *
   * @param filePath the file path for the snapshot.
   * @return a protobuf message.
   */
  public static Message buildSnapshotDone(String filePath) {
    ZabMessage.SnapshotDone done =
      ZabMessage.SnapshotDone.newBuilder().setFilePath(filePath).build();
    return Message.newBuilder().setType(MessageType.SNAPSHOT_DONE)
                               .setSnapshotDone(done).build();
  }
}

