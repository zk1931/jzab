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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persistent variable for Zab.
 */
class PersistentState {
  /**
   * The transaction log.
   */
  protected Log log;

  /**
   * The file to store the last acknowledged epoch.
   */
  private final File fAckEpoch;

  /**
   * The file to store the last proposed epoch.
   */
  private final File fProposedEpoch;

  /**
   * The root directory for all the persistent variables.
   */
  private final File rootDir;

  /**
   * The sub directory for log and snapshots.
   */
  private File dataDir;

  /**
   * The boolean flag indicates whether the state is in state tranferfing mode.
   */
  private boolean isTransferring = false;

  private static final Logger LOG
    = LoggerFactory.getLogger(PersistentState.class);

  public PersistentState(String dir) throws IOException {
    this(new File(dir));
  }

  public PersistentState(File dir) throws IOException {
    this(dir, null);
  }

  PersistentState(File dir, Log log) throws IOException {
    this.rootDir = dir;
    LOG.debug("Trying to create log directory {}", rootDir.getAbsolutePath());
    if (!rootDir.mkdir()) {
      LOG.debug("Creating log directory {} failed, already exists?",
                rootDir.getAbsolutePath());
    }
    this.dataDir = getLatestDataDir();
    if (this.dataDir == null) {
      this.dataDir = getNextDataDir();
      this.dataDir.mkdir();
    }
    this.fAckEpoch = new File(rootDir, "ack_epoch");
    this.fProposedEpoch = new File(rootDir, "proposed_epoch");
    if (log == null) {
      this.log = createLog(this.dataDir);
    } else {
      this.log = log;
    }
  }

  /**
   * Creates or restores the transaction log from a given directory.
   *
   * @param dir the log directory.
   */
  Log createLog(File dir) throws IOException {
    return new RollingLog(dir, ZabConfig.ROLLING_SIZE);
  }

  /**
   * Gets the latest zxid from persistent state. If the log file is not empty,
   * gets the latest zxid from log file since zxid of log is always equal or
   * larger than the zxid of the snapshot file. If the log file is empty, gets
   * the zxid from snapshot file.
   *
   * @return the latest zxid which is guaranteed on disk.
   */
  Zxid getLatestZxid() throws IOException {
    Zxid zxid = this.log.getLatestZxid();
    if (zxid.compareTo(Zxid.ZXID_NOT_EXIST) == 0) {
      zxid = getSnapshotZxid();
    }
    return zxid;
  }

  /**
   * Gets the last acknowledged epoch.
   *
   * @return the last acknowledged epoch.
   * @throws IOException in case of IO failures.
   */
  long getAckEpoch() throws IOException {
    try {
      long ackEpoch = FileUtils.readLongFromFile(this.fAckEpoch);
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
  void setAckEpoch(long ackEpoch) throws IOException {
    // Since the new acknowledged epoch file gets created, we need to fsync
    // the directory.
    FileUtils.writeLongToFile(ackEpoch, this.fAckEpoch);
  }

  /**
   * Gets the last proposed epoch.
   *
   * @return the last proposed epoch.
   * @throws IOException in case of IO failures.
   */
  long getProposedEpoch() throws IOException {
    try {
      long pEpoch = FileUtils.readLongFromFile(this.fProposedEpoch);
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
  void setProposedEpoch(long pEpoch) throws IOException {
    FileUtils.writeLongToFile(pEpoch, this.fProposedEpoch);
    // Since the new proposed epoch file gets created, we need to fsync the
    // directory.
    fsyncDirectory();
  }

  /**
   * Gets last seen configuration.
   *
   * @return the last seen configuration.
   * @throws IOException in case of IO failure.
   */
  ClusterConfiguration getLastSeenConfig() throws IOException {
    File file = getLatestFileWithPrefix(this.rootDir, "cluster_config");
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
   * Gets the last configuration which has version equal or smaller than zxid.
   *
   * @param zxid the zxid.
   * @return the last configuration which is equal or smaller than zxid.
   * @throws IOException in case of IO failure.
   */
  ClusterConfiguration getLastConfigWithin(Zxid zxid) throws IOException {
    String pattern = "cluster_config\\.\\d+_-?\\d+";
    String zxidFileName = "cluster_config." + zxid.toSimpleString();
    String lastFileName = null;

    for (File file : this.rootDir.listFiles()) {
      if (!file.isDirectory() && file.getName().matches(pattern)) {
        String fileName = file.getName();
        if (lastFileName == null && fileName.compareTo(zxidFileName) <= 0) {
          lastFileName = fileName;
        } else if (lastFileName != null &&
                   fileName.compareTo(lastFileName) > 0 &&
                   fileName.compareTo(zxidFileName) <= 0) {
          lastFileName = fileName;
        }
      }
    }
    if (lastFileName == null) {
      return null;
    }
    File file = new File(this.rootDir, lastFileName);
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
    File file = new File(rootDir, String.format("cluster_config.%s", version));
    FileUtils.writePropertiesToFile(conf.toProperties(), file);
    // Since the new config file gets created, we need to fsync the directory.
    fsyncDirectory();
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
    return this.rootDir.listFiles().length == 1;
  }

  /**
   * Turns a temporary snapshot file into a valid snapshot file.
   *
   * @param tempFile the temporary file which stores current state.
   * @param zxid the last applied zxid for state machine.
   * @return the snapshot file.
   */
  File setSnapshotFile(File tempFile, Zxid zxid) throws IOException {
    File snapshot =
      new File(dataDir, String.format("snapshot.%s", zxid.toSimpleString()));
    LOG.debug("Atomically move snapshot file to {}", snapshot);
    FileUtils.atomicMove(tempFile, snapshot);
    // Since the new snapshot file gets created, we need to fsync the directory.
    fsyncDirectory();
    return snapshot;
  }

  /**
   * Gets the last snapshot file.
   *
   * @return the last snapshot file.
   */
  File getSnapshotFile() {
    return getLatestFileWithPrefix(this.dataDir, "snapshot");
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
    return File.createTempFile(prefix, "", this.rootDir);
  }

  File getLogDir() {
    return this.rootDir;
  }

  /**
   * Gets file with highest zxid for given prefix. The file name has format :
   * prefix.zxid.
   *
   * @return the file with highest zxid in its name for given prefix, or null
   * if there's no such files.
   */
  File getLatestFileWithPrefix(File dir, String prefix) {
    List<File> files = getFilesWithPrefix(dir, prefix);
    if (!files.isEmpty()) {
      return files.get(files.size() - 1);
    }
    return null;
  }

  List<File> getFilesWithPrefix(File dir, String prefix) {
    List<File> files = new ArrayList<File>();
    String pattern = prefix + "\\.\\d+_-?\\d+";
    for (File file : dir.listFiles()) {
      if (!file.isDirectory() && file.getName().matches(pattern)) {
        // Only consider those with valid name.
        files.add(file);
      }
    }
    if (!files.isEmpty()) {
      // Picks the last one.
      Collections.sort(files);
    }
    return files;
  }

  // We need also fsync file directory when file gets created. This is related
  // to ZOOKEEPER-2003 https://issues.apache.org/jira/browse/ZOOKEEPER-2003
  void fsyncDirectory() throws IOException {
    try (FileChannel channel = FileChannel.open(this.rootDir.toPath())) {
      channel.force(true);
    }
  }

  /**
   * Begins state transferring mode, all txns and snapshots persisted in state
   * transferring mode will not show up after recovery if you do not call
   * {@link endStateTransfer}.
   */
  void beginStateTransfer() throws IOException {
    this.isTransferring = true;
    this.dataDir =
      Files.createTempDirectory(this.rootDir.toPath(), "tmp_data").toFile();
    this.log = createLog(this.dataDir);
  }

  /**
   * Ends state transferring mode, after calling this function, all the txns
   * and snapshots persisted during the transferring mode will be visible from
   * now.
   */
  void endStateTransfer() throws IOException {
    File nextDir = getNextDataDir();
    FileUtils.atomicMove(this.dataDir, nextDir);
    // Update current data directory.
    this.dataDir = nextDir;
    // Restores transaction log.
    this.log = createLog(dataDir);
    fsyncDirectory();
    this.isTransferring = false;
  }

  /**
   * Whether the persistent state is in state transferring mode or not.
   */
  boolean isInStateTransfer() {
    return this.isTransferring;
  }

  /**
   * Clear all the data in state transferring mode and restores to previous
   * state.
   */
  void undoStateTransfer() throws IOException {
    this.isTransferring = false;
    // Restores data directory.
    this.dataDir = getLatestDataDir();
    // Restores transaction log.
    this.log = createLog(dataDir);
  }

  /**
   * Gets the latest data directory.
   */
  File getLatestDataDir() {
    List<String> files = new ArrayList<String>();
    String pattern = "data\\d+";
    for (File file : this.rootDir.listFiles()) {
      if (file.getName().matches(pattern) && file.isDirectory()) {
        files.add(file.getName());
      }
    }
    if (!files.isEmpty()) {
      // Picks the last one.
      Collections.sort(files);
      return new File(this.rootDir, files.get(files.size() - 1));
    }
    return null;
  }

  /**
   * Gets the next data directory.
   */
  File getNextDataDir() {
    File latest = getLatestDataDir();
    long newID;
    if (latest == null) {
      newID = 0;
    } else {
      newID = Long.parseLong(latest.getName().substring(4)) + 1;
    }
    String suffix = String.format("%015d", newID);
    return new File(this.rootDir, "data" + suffix);
  }


   // In some cases the zxid of the cluster configuration files might be larger
   // than the zxid in log and snapshot, we consider these cluster configuration
   // files invalid. Since everytime we pick the "latest" cluster_config file in
   // directory as current configuration, we need to clean up these invalid
   // cluster_config files.
  void cleanupClusterConfigFiles() throws IOException {
    Zxid latestZxid = getLatestZxid();
    List<File> files = getFilesWithPrefix(this.rootDir, "cluster_config");
    if (files.isEmpty()) {
      LOG.error("There's no cluster_config files in log directory.");
      throw new RuntimeException("There's no cluster_config files!");
    }
    Iterator<File> iter = files.iterator();
    while (iter.hasNext()) {
      File file = iter.next();
      String fileName = file.getName();
      String strZxid = fileName.substring(fileName.indexOf('.') + 1);
      Zxid zxid = Zxid.fromSimpleString(strZxid);
      if (zxid.compareTo(latestZxid) > 0) {
        // Deletes the config file if its zxid is larger than the latest zxid on
        // disk.
        file.delete();
        iter.remove();
      }
    }
    if (files.isEmpty()) {
      LOG.error("There's no cluster_config files after cleaning up.");
      throw new RuntimeException("There's no cluster_config files!");
    }
    // Persists changes.
    fsyncDirectory();
  }
}
