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
 * Stores the statistics about follower.
 */
class FollowerStat {
  private int lastProposedEpoch = -1;
  private int lastAckedEpoch = -1;
  private long lastHeartbeatTime;
  private Zxid lastProposedZxid = null;

  public FollowerStat(int epoch) {
    setLastProposedEpoch(epoch);
    updateHeartbeatTime();
  }

  long getLastHeartbeatTime() {
    return this.lastHeartbeatTime;
  }

  void updateHeartbeatTime() {
    this.lastHeartbeatTime = System.nanoTime();
  }

  int getLastProposedEpoch() {
    return this.lastProposedEpoch;
  }

  void setLastProposedEpoch(int epoch) {
    this.lastProposedEpoch = epoch;
  }

  int getLastAckedEpoch() {
    return this.lastAckedEpoch;
  }

  void setLastAckedEpoch(int epoch) {
    this.lastAckedEpoch = epoch;
  }

  Zxid getLastProposedZxid() {
    return this.lastProposedZxid;
  }

  void setLastProposedZxid(Zxid zxid) {
    this.lastProposedZxid = zxid;
  }
}

