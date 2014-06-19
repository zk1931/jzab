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
 * Simply choose the leader in a round robin way.
 */
public class RoundRobinElection implements Election {
  private int round = 0;
  private int lastEpoch = -1;

  @Override
  public void initialize(ServerState state, ElectionCallback cb) {
    if (state.getProposedEpoch() != this.lastEpoch) {
      // Reset round to zero once change to a new epoch.
      this.round = 0;
      this.lastEpoch = state.getProposedEpoch();
    }
    int idx = this.round % state.getEnsembleSize();
    cb.leaderElected(state.getServerList().get(idx));
    this.round++;
  }

  @Override
  public void processMessage() {
    // Ignore the process message.
  }

}
