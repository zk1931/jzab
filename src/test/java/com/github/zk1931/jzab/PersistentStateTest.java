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
import org.junit.Assert;
import org.junit.Test;

/**
 *  Test PersistentState.
 */
public class PersistentStateTest extends TestBase {
  @Test
  public void testEmpty() throws IOException {
    // Test for empty directory.
    PersistentState persistence = new PersistentState(getDirectory());
    Assert.assertEquals(true, persistence.isEmpty());
    Assert.assertEquals(Zxid.ZXID_NOT_EXIST,
                        persistence.getLog().getLatestZxid());
    Assert.assertEquals(-1, persistence.getProposedEpoch());
    Assert.assertEquals(-1, persistence.getAckEpoch());
    Assert.assertEquals(null, persistence.getLastSeenConfig());
  }

  @Test
  public void testReadWrite() throws IOException {
    // Test Read/Write log.
    PersistentState persistence = new PersistentState(getDirectory());
    persistence.setProposedEpoch(2);
    persistence.setAckEpoch(3);
    Transaction txn = new Transaction(new Zxid(0, 0),
                                      ByteBuffer.wrap("txn".getBytes()));
    persistence.getLog().append(txn);
    ClusterConfiguration cnf =
      new ClusterConfiguration(new Zxid(0, 1), new ArrayList<String>(), "v");
    persistence.setLastSeenConfig(cnf);
    Assert.assertEquals(2, persistence.getProposedEpoch());
    Assert.assertEquals(3, persistence.getAckEpoch());
    Assert.assertEquals(new Zxid(0, 0), persistence.getLog().getLatestZxid());
    cnf = persistence.getLastSeenConfig();
    Assert.assertEquals(new Zxid(0, 1), cnf.getVersion());
    Assert.assertEquals("v", cnf.getServerId());
    Assert.assertEquals(true, cnf.getPeers().isEmpty());
  }

  @Test
  public void testRestore() throws IOException {
    // Test restore from a log.
    PersistentState persistence = new PersistentState(getDirectory());
    persistence.setProposedEpoch(2);
    persistence.setAckEpoch(3);
    Transaction txn = new Transaction(new Zxid(0, 0),
                                      ByteBuffer.wrap("txn".getBytes()));
    persistence.getLog().append(txn);
    ClusterConfiguration cnf =
      new ClusterConfiguration(new Zxid(0, 1), new ArrayList<String>(), "v");
    persistence.setLastSeenConfig(cnf);

    // Recovers.
    persistence = new PersistentState(getDirectory());
    Assert.assertEquals(2, persistence.getProposedEpoch());
    Assert.assertEquals(3, persistence.getAckEpoch());
    Assert.assertEquals(new Zxid(0, 0), persistence.getLog().getLatestZxid());
    cnf = persistence.getLastSeenConfig();
    Assert.assertEquals(new Zxid(0, 1), cnf.getVersion());
    Assert.assertEquals("v", cnf.getServerId());
    Assert.assertEquals(true, cnf.getPeers().isEmpty());
  }
}
