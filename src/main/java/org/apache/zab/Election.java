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

/**
 * Interface for the leader election implementation.
 */
public interface Election {
  /**
   * Starts one round leader election.
   *
   * @param state the state provides needed information for leader election
   * @param cb the callback which will be called to notify the elected leader
   */
  void initialize(ServerState state, ElectionCallback cb) throws Exception;

  /**
   * Processes messages of election. Now the message format is undefined.
   */
  void processMessage();

  /**
   * Interface used by election. Once a leader is elected, the callback will
   * be invoked.
   */
  public interface ElectionCallback {
    /**
     * Callback that will be called once the leader is elected.
     *
     * @param serverId the id of newly elected leader
     */
    void leaderElected(String serverId);
  }
}
