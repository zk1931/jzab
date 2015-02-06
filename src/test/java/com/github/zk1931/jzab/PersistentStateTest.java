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
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Test PersistentState.
 */
public class PersistentStateTest extends TestBase {

  private static final Logger LOG = LoggerFactory.getLogger(LogTest.class);

  Class<Log> logClass;

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

  @Test
  public void testStateTransfering() throws IOException {
    PersistentState persistence = new PersistentState(getDirectory());
    appendTxns(persistence.getLog(), new Zxid(0, 0), 2);
    Assert.assertEquals(new Zxid(0, 1), persistence.getLatestZxid());
    /*
     * Test case 1:
     *
     * Appends 100 txns in state transferring mode then ends transferring mode
     * and verify they show up in log.
     */
    // Begins state transferring mode.
    persistence.beginStateTransfer();
    // Make sure it's in state transferring mode.
    Assert.assertTrue(persistence.isInStateTransfer());
    // Appends 100 txns.
    appendTxns(persistence.getLog(), new Zxid(0, 0), 100);
    // Make sure the appended txns show up.
    Assert.assertEquals(new Zxid(0, 99), persistence.getLatestZxid());
    // Ends state transferring.
    persistence.endStateTransfer();
    // Restores state.
    persistence = new PersistentState(getDirectory());
    // Make sure the appended txns show up.
    Assert.assertEquals(new Zxid(0, 99), persistence.getLatestZxid());

    /*
     * Test case 2:
     *
     * Appends 10 txns in state transferring mode, but without calling
     * endStateTransfering we restore persistent state and verify the
     * 10 txns will not show up.
     */
    // Begins state transferring mode again.
    persistence.beginStateTransfer();
    // Make sure it's in state transferring mode.
    Assert.assertTrue(persistence.isInStateTransfer());
    // Appends 10 txns.
    appendTxns(persistence.getLog(), new Zxid(0, 0), 10);
    // Make sure the appended txns show up.
    Assert.assertEquals(new Zxid(0, 9), persistence.getLatestZxid());
    // But we'll not call endStateTransfering, the intermediate result will be
    // discarded.
    persistence = new PersistentState(getDirectory());
    // Still show old data.
    Assert.assertEquals(new Zxid(0, 99), persistence.getLatestZxid());

    /*
     * Test case 3:
     *
     * Appends 10 txns in state transferring mode, but without calling
     * endStateTransfering we undo the state transferring explicitly and verify
     * the 10 txns will not show up.
     */
    persistence.beginStateTransfer();
    // Appends 10 txns.
    appendTxns(persistence.getLog(), new Zxid(0, 0), 10);
    // Make sure the appended txns show up.
    Assert.assertEquals(new Zxid(0, 9), persistence.getLatestZxid());
    Assert.assertTrue(persistence.isInStateTransfer());
    // Undo state transferring manually.
    persistence.undoStateTransfer();
    // Still show old data.
    Assert.assertEquals(new Zxid(0, 99), persistence.getLatestZxid());
  }

  @Test
  public void testClusterConfigFiles() throws IOException {
    Set<String> peers = new HashSet<String>();
    ClusterConfiguration cnf =
      new ClusterConfiguration(new Zxid(0, -1), peers, "");
    PersistentState persistence = new PersistentState(getDirectory());
    persistence.setLastSeenConfig(cnf);
    cnf = new ClusterConfiguration(new Zxid(0, 1), peers, "");
    persistence.setLastSeenConfig(cnf);
    // Make sures <0, -1> is smaller than <0, 1>.
    Assert.assertEquals(persistence.getLastSeenConfig().getVersion(),
                        new Zxid(0, 1));
  }

  @Test
  public void testCleanupClusterConfigFiles() throws IOException {
    PersistentState persistence = new PersistentState(getDirectory());
    Set<String> peers = new HashSet<String>();
    ClusterConfiguration cnf =
      new ClusterConfiguration(new Zxid(0, -1), peers, "");
    // Initialy set version to <0, -1>
    persistence.setLastSeenConfig(cnf);

    // Appends 3 txns so the latest zxid is <0, 2>
    appendTxns(persistence.getLog(), new Zxid(0, 0), 3);

    // Sets config files with version <0, 3> ~ <0, 5>
    for (int i = 3; i < 6; ++i) {
      cnf = new ClusterConfiguration(new Zxid(0, i), peers, "");
      persistence.setLastSeenConfig(cnf);
    }
    Assert.assertEquals(persistence.getLastSeenConfig().getVersion(),
                        new Zxid(0, 5));

    // Latest zxid in log is <0, 2>, the latest zxid of config is
    // <0, 3>, this one will be deleted.
    persistence.cleanupClusterConfigFiles();
    Assert.assertEquals(persistence.getLastSeenConfig().getVersion(),
                        new Zxid(0, -1));


    cnf = new ClusterConfiguration(new Zxid(0, 2), peers, "");
    persistence.setLastSeenConfig(cnf);
    persistence.cleanupClusterConfigFiles();
    // After cleaning up, the version of config should be <0, 2>.
    Assert.assertEquals(persistence.getLastSeenConfig().getVersion(),
                        new Zxid(0, 2));
  }

  @Test
  public void testLastConfig() throws IOException {
    PersistentState persistence = new PersistentState(getDirectory());
    Set<String> peers = new HashSet<String>();
    ClusterConfiguration cnf =
      new ClusterConfiguration(new Zxid(0, 1), peers, "");
    // Initialy set version to <0, 1>
    persistence.setLastSeenConfig(cnf);

    cnf = persistence.getLastConfigWithin(new Zxid(0, 1));
    // Now the last config within <0, 1> should be <0, 1>
    Assert.assertEquals(new Zxid(0, 1), cnf.getVersion());

    cnf = new ClusterConfiguration(new Zxid(0, 2), peers, "");
    persistence.setLastSeenConfig(cnf);
    // Now the last config within <0, 2> should be <0, 2>
    cnf = persistence.getLastConfigWithin(new Zxid(0, 2));
    Assert.assertEquals(new Zxid(0, 2), cnf.getVersion());
    cnf = persistence.getLastConfigWithin(new Zxid(0, 1));
    // Now the last config within <0, 1> should be <0, 1>
    Assert.assertEquals(new Zxid(0, 1), cnf.getVersion());

    cnf = persistence.getLastConfigWithin(new Zxid(0, 0));
    Assert.assertTrue(cnf == null);
  }
}
