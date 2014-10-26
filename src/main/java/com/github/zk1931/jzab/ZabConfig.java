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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Simple wrapper on Properties, provides a way to query configuration.
 */
public class ZabConfig {

  /**
   * Maximum number of pending requests allowed for each server.
   */
  static final int MAX_PENDING_REQS = 10000;

  protected Properties prop;

  protected List<String> peers = null;

  /**
   * Constructs the config object.
   *
   * @param prop the Properties object stores the configuration.
   */
  public ZabConfig(Properties prop) {
    this.prop = prop;

    String servers = this.prop.getProperty("servers");

    if (servers == null) {
      peers = Arrays.asList(new String[0]);
    } else {
      peers = Arrays.asList(servers.split(";"));
    }
    // Sorts servers alphabetically.
    Collections.sort(peers);
  }

  /**
   * Gets the address of peers.
   *
   * @return a list of address(id) of other servers.
   */
  public List<String> getPeers() {
    return peers;
  }

  /**
   * Gets the id of itself.
   *
   * @return a string represents the id of itself
   */
  public String getServerId() {
    return this.prop.getProperty("serverId");
  }

  /**
   * Gets the size of the ensemble.
   *
   * @return the number of the nodes in the ensemble.
   */
  public int getEnsembleSize() {
    return getPeers().size();
  }

  /**
   * Gets the directory for storing transaction log.
   *
   * @return the directory for storing transaction log, if it's undefined,
   * return the current user's working directory.
   */
  public String getLogDir() {
    return this.prop.getProperty("logdir", System.getProperty("user.dir"));
  }

  /**
   * Gets the timeout of heartbeat messages (default is 1000 milliseconds).
   *
   * @return the timeout in milliseconds
   */
  public int getTimeoutMs() {
    return Integer.parseInt(this.prop.getProperty("timeout_ms", "1000"));
  }

  /**
   * Gets the timeout for synchronizing peers (default is 3000 milliseconds).
   *
   * @return the timeout in milliseconds.
   */
  public int getMinSyncTimeoutMs() {
    return Integer.parseInt(prop.getProperty("min_sync_timeout_ms", "3000"));
  }

  /**
   * Gets the address of peer you want to join in.
   *
   * @return the address of the peer.
   */
  public String getJoinPeer() {
    return this.prop.getProperty("joinPeer");
  }

  /**
   * The threshold for taking snapshot.
   *
   * @return long integer of threshold, we'll take snapshot once this number of
   * bytes transactions are delivered to application since last snapshot. If
   * it's set to -1, then we won't take snapshot.
   */
  public long getSnapshotThreshold() {
    return Long.parseLong(this.prop.getProperty("snapshot_threshold_bytes",
                                                "-1"));
  }

  /**
   * Gets the method for leader election. (All the servers in the cluster MUST
   * use the SAME election method.)
   *
   * For now, we have 2 election methods:
   *
   * 1)fast_leader_election (default)
   * 2)round_robin_election
   *
   * @return the configured election method.
   */
  public String getElectionMethod() {
    return this.prop.getProperty("election", "fast_leader_election");
  }
}
