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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import com.github.zk1931.jzab.proto.ZabMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configurations(ensemble of cluster, serverId, version).
 */
class ClusterConfiguration {
  private Zxid version;
  private final List<String> peers;
  private final String serverId;

  private static final Logger LOG =
      LoggerFactory.getLogger(ClusterConfiguration.class);

  public ClusterConfiguration(Zxid version,
                              List<String> peers,
                              String serverId) {
    this.version = version;
    this.peers = new ArrayList<String>(peers);
    this.serverId = serverId;
  };

  public Zxid getVersion() {
    return this.version;
  }

  public void setVersion(Zxid newVersion) {
    this.version = newVersion;
  }

  public List<String> getPeers() {
    return this.peers;
  }

  public String getServerId() {
    return this.serverId;
  }

  public void addPeer(String peer) {
    peers.add(peer);
  }

  public void removePeer(String peer) {
    this.peers.remove(peer);
  }

  public Properties toProperties() {
    Properties prop = new Properties();
    StringBuilder strBuilder = new StringBuilder();
    String strVersion = this.version.toSimpleString();
    if (!this.peers.isEmpty()) {
      strBuilder.append(this.peers.get(0));
      for (int i = 1; i < this.peers.size(); ++i) {
        strBuilder.append("," + this.peers.get(i));
      }
    }
    prop.setProperty("peers", strBuilder.toString());
    prop.setProperty("version", strVersion);
    prop.setProperty("serverId", this.serverId);
    return prop;
  }

  public static ClusterConfiguration fromProperties(Properties prop) {
    String strPeers = prop.getProperty("peers");
    Zxid version = Zxid.fromSimpleString(prop.getProperty("version"));
    String serverId = prop.getProperty("serverId");
    List<String> peerList;
    if (strPeers.equals("")) {
      peerList = new ArrayList<String>();
    } else {
      peerList = new ArrayList<String>(Arrays.asList(strPeers.split(",")));
    }
    return new ClusterConfiguration(version, peerList, serverId);
  }

  public static
  ClusterConfiguration fromProto(ZabMessage.ClusterConfiguration cnf,
                                 String serverId) {
    Zxid version = MessageBuilder.fromProtoZxid(cnf.getVersion());
    return new ClusterConfiguration(version, cnf.getServersList(), serverId);
  }

  public ZabMessage.ClusterConfiguration toProto() {
    return MessageBuilder.buildConfig(this);
  }

  public ByteBuffer toByteBuffer() {
    return ByteBuffer.wrap(toProto().toByteArray());
  }

  public static ClusterConfiguration fromByteBuffer(ByteBuffer buffer,
                                                    String serverId)
      throws IOException {
    byte[] bufArray = new byte[buffer.remaining()];
    buffer.get(bufArray);
    return fromProto(ZabMessage.ClusterConfiguration.parseFrom(bufArray),
                     serverId);
  }

  public boolean contains(String peerId) {
    return this.peers.contains(peerId);
  }

  @Override
  public String toString() {
    return toProperties().toString();
  }

  /**
   * Gets the minimal quorum size.
   */
  public int getQuorumSize() {
    int clusterSize = this.getPeers().size();
    if (clusterSize == 0) {
      return 0;
    } else {
      return clusterSize / 2 + 1;
    }
  }

  public ClusterConfiguration clone() {
    return new ClusterConfiguration(version, new ArrayList<String>(peers),
                                    serverId);
  }
}
