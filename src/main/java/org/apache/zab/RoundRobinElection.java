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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simply choose the leader in a round robin way.
 */
public class RoundRobinElection implements Election {
  private int round = 0;
  private long lastEpoch = -1;

  private static final Logger LOG =
      LoggerFactory.getLogger(RoundRobinElection.class);

  @Override
  public String electLeader(PersistentState persistence) throws Exception {
    ClusterConfiguration cnf = persistence.getLastSeenConfig();
    if (persistence.getProposedEpoch() != this.lastEpoch) {
      LOG.debug("Last proposed epoch is changed from {} to {}, resets round.",
                this.lastEpoch,
                persistence.getProposedEpoch());
      // Reset round to zero once change to a new epoch.
      this.round = 0;
      this.lastEpoch = persistence.getProposedEpoch();
    } else {
      try {
        Thread.sleep((long)(Math.random() * 500));
      } catch (InterruptedException e) {
        LOG.debug("Interrupted exception in RoundRobinElection.");
      } catch (Exception e) {
        throw e;
      }
    }
    int idx = this.round % cnf.getPeers().size();
    this.round++;
    return cnf.getPeers().get(idx);
  }

  @Override
  public void processMessage() {
    // Ignore the process message.
  }

}
