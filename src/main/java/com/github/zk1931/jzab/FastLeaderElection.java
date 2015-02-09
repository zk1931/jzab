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

import com.github.zk1931.jzab.proto.ZabMessage;
import com.github.zk1931.jzab.proto.ZabMessage.Message;
import com.github.zk1931.jzab.proto.ZabMessage.Message.MessageType;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fast leader election implementation. Fast leader election will try its best
 * effort to elect the leader with the "best" history to minimize the
 * synchronization cost.
 */
class FastLeaderElection implements Election {
  static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);
  // Fast leader election needs transport to exchange vote information.
  final Transport transport;
  // The queue for incoming messages(both queries and replies).
  final BlockingQueue<MessageTuple> messageQueue;
  // The last vote for this server.
  private VoteInfo voteInfo = null;
  // Round number.
  private long round = 0;
  // Persistent state.
  private PersistentState persistence;
  // Message queue filter for fast leader election.
  private final ElectioneerFilter filter;

  FastLeaderElection(PersistentState persistence, Transport transport,
                     BlockingQueue<MessageTuple> messageQueue) {
    this.transport = transport;
    this.messageQueue = messageQueue;
    this.persistence = persistence;
    filter = new ElectioneerFilter(messageQueue);
  }

  @Override
  public String electLeader() throws Exception {
    ClusterConfiguration clusterConfig = persistence.getLastSeenConfig();
    Zxid lastZxid = persistence.getLog().getLatestZxid();
    String serverId = clusterConfig.getServerId();
    long ackEpoch = persistence.getAckEpoch();
    // The map stores all the votes from the servers who are in the same round.
    HashMap<String, VoteInfo> receivedVotes = new HashMap<String, VoteInfo>();
    // Everytime enters election, increments the round number.
    this.round++;
    // The first vote should be itself.
    this.voteInfo = new VoteInfo(serverId, ackEpoch, lastZxid, round, true);
    int timeoutMs = 100;
    int maxTimeoutMs = 1600;
    // Broadcasts its own vote first.
    broadcast(clusterConfig);

    while (true) {
      MessageTuple msgTuple;
      try {
        msgTuple = filter.getMessage(timeoutMs);
      } catch (TimeoutException ex) {
        // Timeout without any incoming vote message.
        if (receivedVotes.size() >= clusterConfig.getQuorumSize()) {
          // If we've already received votes from a quorum of servers who are in
          // the same round, then we assume probably we find the server who has
          // the "best" history.
          this.voteInfo.electing = false;
          return this.voteInfo.vote;
        } else {
          // No any incoming message after certain timeout, broadcasts its own
          // vote and increase its timeout.
          broadcast(clusterConfig);
          timeoutMs =
            (timeoutMs * 2 > maxTimeoutMs)? maxTimeoutMs : 2 * timeoutMs;
        }
        continue;
      }
      VoteInfo vote = VoteInfo.fromMessage(msgTuple.getMessage());
      String source = msgTuple.getServerId();
      if (!clusterConfig.contains(source)) {
        // If the vote comes from a server who is not in your curernt
        // configuration, ignores it.
        LOG.debug("The vote is from server {} who is not in current " +
            "configuration, ignores it.", source);
        continue;
      }
      if (vote.electing) {
        // The vote comes from a server who is also in electing phase.
        if (vote.round > this.voteInfo.round) {
          LOG.debug("The round of peer's vote {} is larger than itself {}",
                    vote.round, this.voteInfo.round);
          this.round = vote.round;
          // Updates its round number.
          this.voteInfo.round = vote.round;
          // Since the round number has been changed, we need to clear the map.
          receivedVotes.clear();
          if (this.voteInfo.compareTo(vote) < 0) {
            // Updates its vote if the peer's vote is better.
            this.voteInfo = vote;
          }
          broadcast(clusterConfig);
        } else if (vote.round == this.voteInfo.round &&
                   this.voteInfo.compareTo(vote) < 0) {
          // Updates its vote if the peer's vote is better.
          this.voteInfo = vote;
          broadcast(clusterConfig);
        } else if(vote.round < this.voteInfo.round) {
          // Ignores if the peer's round is smaller than itself.
          continue;
        } else if (vote.round == this.voteInfo.round &&
                   this.voteInfo.compareTo(vote) > 0) {
          broadcast(clusterConfig);
        }
        // Updates the received votes.
        receivedVotes.put(source, vote);
        if (receivedVotes.size() == clusterConfig.getPeers().size()) {
          this.voteInfo.electing = false;
          return this.voteInfo.vote;
        }
      } else {
        // Which means the peer is in non-electing phase.
        this.voteInfo = vote;
        this.voteInfo.electing = false;
        return this.voteInfo.vote;
      }
    }
  }

  @Override
  public void reply(MessageTuple tuple) {
    if (tuple.getMessage().getElectionInfo().getIsElecting() &&
        this.voteInfo != null) {
      // If it's the server first time joining a cluster, it won't
      // initialize its vote information until first synchronization from
      // leader is done. The vote might be null before the synchronization is
      // done. In this case, we won't reply its vote to other querier.
      LOG.debug("Replies to {} with leader info : {}",
                tuple.getServerId(),
                voteInfo.vote);
      this.transport.send(tuple.getServerId(), this.voteInfo.toMessage());
    }
  }

  @Override
  public void specifyLeader(String leader) {
    this.voteInfo = new VoteInfo(leader, -1, Zxid.ZXID_NOT_EXIST, -1, false);
  }

  // Broadcasts its vote to all the peers in current configuration.
  void broadcast(ClusterConfiguration config) {
    Message vote = voteInfo.toMessage();
    for (String server : config.getPeers()) {
      this.transport.send(server, vote);
    }
  }

  /**
   * The information of vote.
   */
  static class VoteInfo implements Comparable<VoteInfo> {
    final String vote;
    final long ackEpoch;
    final Zxid zxid;
    long round;
    boolean electing;

    VoteInfo(String vote,
             long ackEpoch,
             Zxid zxid,
             long round,
             boolean electing) {
      this.vote = vote;
      this.ackEpoch = ackEpoch;
      this.zxid = zxid;
      this.round = round;
      this.electing = electing;
    }

    Message toMessage() {
      return MessageBuilder.buildElectionInfo(vote, zxid, ackEpoch, round,
                                              electing);
    }

    // Compares two votes. The order of the comparison is :
    //  ackEpoch -> lastZxid -> serverId
    @Override
    public int compareTo(VoteInfo vi) {
      if (ackEpoch != vi.ackEpoch) {
        return (int)(this.ackEpoch - vi.ackEpoch);
      }
      if (!this.zxid.equals(vi.zxid)) {
        return this.zxid.compareTo(vi.zxid);
      }
      return this.vote.compareTo(vi.vote);
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || !(o instanceof VoteInfo)) {
        return false;
      }
      return compareTo((VoteInfo)o) == 0;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    static VoteInfo fromMessage(Message msg) {
      ZabMessage.ElectionInfo info = msg.getElectionInfo();
      return new VoteInfo(info.getVote(),
                          info.getAckEpoch(),
                          MessageBuilder.fromProtoZxid(info.getZxid()),
                          info.getRound(),
                          info.getIsElecting());
    }
  }

  /**
   * This filter filters any message except the ELECTION_INFO message.
   */
  class ElectioneerFilter extends MessageQueueFilter {
    ElectioneerFilter(BlockingQueue<MessageTuple> msgQueue) {
      super(msgQueue);
    }

    @Override
    protected MessageTuple getMessage(int timeoutMs)
        throws InterruptedException, TimeoutException {
      int startMs = (int)(System.nanoTime() / 1000000);
      while (true) {
        int nowMs = (int)(System.nanoTime() / 1000000);
        int remainMs = timeoutMs - (nowMs - startMs);
        if (remainMs < 0) {
          remainMs = 0;
        }
        MessageTuple tuple = super.getMessage(remainMs);
        Message msg = tuple.getMessage();
        if (msg.getType() == MessageType.ELECTION_INFO) {
          // Got what we want, return it to caller.
          return tuple;
        } else if (msg.getType() == MessageType.DISCONNECTED) {
          transport.clear(msg.getDisconnected().getServerId());
        }
      }
    }
  }
}
