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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persistent variable for Zab.
 */
public class PersistentState {
  /**
   * The transaction log.
   */
  protected final Log log;

  /**
   * The file to store the last acknowledged epoch.
   */
  private final File fAckEpoch;

  /**
   * The file to store the last proposed epoch.
   */
  private final File fProposedEpoch;

  /**
   * The directory for all the persistent variables.
   */
  private final File logDir;

  private static final Logger LOG
    = LoggerFactory.getLogger(PersistentState.class);

  public PersistentState(String dir) throws IOException {
    this(dir, null);
  }

  public PersistentState(File dir) throws IOException {
    this(dir, null);
  }

  public PersistentState(String dir, Log log) throws IOException {
    this(new File(dir), log);
  }

  public PersistentState(File dir, Log log) throws IOException {
    this.logDir = dir;
    LOG.debug("Trying to create log directory {}", logDir.getAbsolutePath());
    if (!logDir.mkdir()) {
      LOG.debug("Creating log directory {} failed, already exists?",
                logDir.getAbsolutePath());
    }
    this.fAckEpoch = new File(logDir, "ack_epoch");
    this.fProposedEpoch = new File(logDir, "proposed_epoch");
    File logFile = new File(logDir, "transaction.log");
    if (log != null) {
      this.log = log;
    } else {
      this.log = new SimpleLog(logFile);
    }
  }

  /**
   * Gets the last acknowledged epoch.
   *
   * @return the last acknowledged epoch.
   * @throws IOException in case of IO failures.
   */
  int getAckEpoch() throws IOException {
    try {
      int ackEpoch = FileUtils.readIntFromFile(this.fAckEpoch);
      return ackEpoch;
    } catch (FileNotFoundException e) {
      LOG.debug("File not exist, initialize acknowledged epoch to -1");
      return -1;
    } catch (IOException e) {
      LOG.error("IOException encountered when access acknowledged epoch");
      throw e;
    }
  }

  /**
   * Updates the last acknowledged epoch.
   *
   * @param ackEpoch the updated last acknowledged epoch.
   * @throws IOException in case of IO failures.
   */
  void setAckEpoch(int ackEpoch) throws IOException {
    FileUtils.writeIntToFile(ackEpoch, this.fAckEpoch);
  }

  /**
   * Gets the last proposed epoch.
   *
   * @return the last proposed epoch.
   * @throws IOException in case of IO failures.
   */
  int getProposedEpoch() throws IOException {
    try {
      int pEpoch = FileUtils.readIntFromFile(this.fProposedEpoch);
      return pEpoch;
    } catch (FileNotFoundException e) {
      LOG.debug("File not exist, initialize acknowledged epoch to -1");
      return -1;
    } catch (IOException e) {
      LOG.error("IOException encountered when access acknowledged epoch");
      throw e;
    }
  }

  /**
   * Updates the last proposed epoch.
   *
   * @param pEpoch the updated last proposed epoch.
   * @throws IOException in case of IO failure.
   */
  void setProposedEpoch(int pEpoch) throws IOException {
    FileUtils.writeIntToFile(pEpoch, this.fProposedEpoch);
  }

  /**
   * Gets last seen configuration.
   *
   * @return the last seen configuration.
   * @throws IOException in case of IO failure.
   */
  ClusterConfiguration getLastSeenConfig() throws IOException {
    File file = getLatestFileWithPrefix("cluster_config");
    if (file == null) {
      return null;
    }
    try {
      Properties prop = FileUtils.readPropertiesFromFile(file);
      return ClusterConfiguration.fromProperties(prop);
    } catch (FileNotFoundException e) {
      LOG.debug("AckConfig file doesn't exist, probably it's the first time" +
          "bootup.");
    }
    return null;
  }

  /**
   * Updates the last seen configuration.
   *
   * @param conf the updated configuration.
   * @throws IOException in case of IO failure.
   */
  void setLastSeenConfig(ClusterConfiguration conf) throws IOException {
    String version = conf.getVersion().toSimpleString();
    File file = new File(logDir, String.format("cluster_config.%s", version));
    FileUtils.writePropertiesToFile(conf.toProperties(), file);
  }

  /**
   * Gets the transaction log.
   *
   * @return the transaction log.
   */
  Log getLog() {
    return this.log;
  }

  /**
   * Checks if the log directory is empty.
   *
   * @return true if it's empty.
   */
  boolean isEmpty() {
    return this.logDir.listFiles().length == 1;
  }

  /**
   * Turns a temporary snapshot file into a valid snapshot file.
   *
   * @param tempFile the temporary file which stores current state.
   * @param zxid the last applied zxid for state machine.
   */
  void setSnapshotFile(File tempFile, Zxid zxid) throws IOException {
    File snapshot =
      new File(logDir, String.format("snapshot.%s", zxid.toSimpleString()));
    LOG.debug("Atomically move snapshot file to {}", snapshot);
    FileUtils.atomicMove(tempFile, snapshot);
  }

  /**
   * Gets the last snapshot file.
   *
   * @return the last snapshot file.
   */
  File getSnapshotFile() {
    return getLatestFileWithPrefix("snapshot");
  }

  /**
   * Gets the last zxid of transaction which is guaranteed in snapshot.
   *
   * @return the last zxid of transaction which is guarantted to be applied.
   */
  Zxid getSnapshotZxid() {
    File snapshot = getSnapshotFile();
    if (snapshot == null) {
      return Zxid.ZXID_NOT_EXIST;
    }
    String fileName = snapshot.getName();
    String strZxid = fileName.substring(fileName.indexOf('.') + 1);
    return Zxid.fromSimpleString(strZxid);
  }

  /**
   * Creates a temporary file in log directory with given prefix.
   *
   * @param prefix the prefix of the file.
   */
  File createTempFile(String prefix) throws IOException {
    return File.createTempFile(prefix, "", this.logDir);
  }

  File getLogDir() {
    return this.logDir;
  }


  /**
   * Gets file with highest zxid for given prefix. The file name has format :
   * prefix.zxid.
   *
   * @return the file with highest zxid in its name for given prefix, or null
   * if there's no such files.
   */
  File getLatestFileWithPrefix(String prefix) {
    List<File> files = new ArrayList<File>();
    String pattern = prefix + "\\.\\d+_\\d+";
    for (File file : this.logDir.listFiles()) {
      if (!file.isDirectory() &&
          file.getName().matches(pattern)) {
        // Only consider those with valid name.
        files.add(file);
      }
    }
    if (!files.isEmpty()) {
      // Picks the last one.
      Collections.sort(files);
      return files.get(files.size() - 1);
    }
    return null;
  }
}

