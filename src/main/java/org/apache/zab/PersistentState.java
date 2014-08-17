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
   * The file to store the last acknowledged config.
   */
  private final File fLastSeenConfig;

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
    this.fLastSeenConfig = new File(logDir, "cluster_config");
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
    try {
      Properties prop = FileUtils.readPropertiesFromFile(this.fLastSeenConfig);
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
    FileUtils.writePropertiesToFile(conf.toProperties(), this.fLastSeenConfig);
  }

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
}

