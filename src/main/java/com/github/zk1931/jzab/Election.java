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

/**
 * Interface for the leader election implementation.
 */
interface Election {
  /**
   * Starts one round leader election.
   *
   * @return the elected leader.
   * @throws Exception in case of exception.
   */
  String electLeader() throws Exception;

  /**
   * Replies its vote to peer. The leader/follower might receive election
   * message in non-electing phase, it ask Election object to reply its vote.
   *
   * @param tuple the message from the querier.
   */
  void reply(MessageTuple tuple);

  /**
   * Specifies the leader explicitly. If the server starts by joining the
   * cluster, it doesn't need to go through the leader election. However,
   * the other servers who go back to recovery phase might ask it about
   * the leader information. They can initialize them the knowledge about
   * leader explicitly once they join a cluster.
   */
  void specifyLeader(String leader);
}
