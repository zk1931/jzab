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

import com.github.zk1931.jzab.Log.DivergingTuple;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rolling log implementation. It's a wrapper of SimpleLog. It maintains
 * a list of log files, once the size of the log file reaches certain threshold,
 * we'll roll the log.
 */
class RollingLog implements Log {
  private static final Logger LOG = LoggerFactory.getLogger(RollingLog.class);

  /**
   * The maximum size for each log file. We'll roll the file once current file
   * reaches the this threshold. (default to 1GB)
   */
  private final long rollingSize;

  /**
   * The list of log files, they are sorted by zxid order.
   */
  private final List<File> logFiles = new ArrayList<File>();

  /**
   * The current log. Transaction will be appended to this log file.
   */
  SimpleLog currentLog;

  /**
   * The log directory for all the log files.
   */
  private final File logDir;

  /**
   * The last seen zxid, used to avoid appending duplicate transactions.
   */
  private Zxid lastSeenZxid = null;

  /**
   * Creates a RollingLog object.
   *
   * @param logDir the directory contains the rolling log files.
   * @param rollingSize the size threshold for rolling a new log.
   * @throws IOException in case of IO failure
   */
  public RollingLog(File logDir, long rollingSize) throws IOException {
    this.logDir = logDir;
    this.rollingSize =  rollingSize;
    // Initialize from log directory.
    initFromDir();
    this.currentLog = getLastLog();
    this.lastSeenZxid = getLatestZxid();
  }

  /**
   * Closes the log file and release the resource.
   *
   * @throws IOException in case of IO failure
   */
  @Override
  public void close() throws IOException {
    if (this.currentLog != null) {
      this.currentLog.close();
      this.currentLog = null;
    }
  }

  /**
   * Appends a request to transaction log.
   *
   * @param txn the transaction which will be added to log.
   * @throws IOException in case of IO failure
   */
  @Override
  public void append(Transaction txn) throws IOException {
    if (this.lastSeenZxid.compareTo(txn.getZxid()) >= 0) {
      String exStr = String.format("The zxid %s is not larger than last seen"
          + " zxid %s in the log.", txn.getZxid(), lastSeenZxid);
      throw new RuntimeException(exStr);
    }
    if (currentLog == null || currentLog.length() >= this.rollingSize) {
      Zxid zxid = txn.getZxid();
      // Close the old one if any.
      this.close();
      File logFile = new File(logDir, "transaction." + zxid.toSimpleString());
      LOG.debug("Rolling to the new log {}.", logFile.getName());
      // Adds new created log file to list.
      this.logFiles.add(logFile);
      this.currentLog = new SimpleLog(logFile);
    }
    this.lastSeenZxid = txn.getZxid();
    this.currentLog.append(txn);
  }

  /**
   * Truncates this transaction log at the given zxid.
   * This method deletes all the transactions with zxids
   * higher than the given zxid.
   *
   * @param zxid the transaction id.
   * @throws IOException in case of IO failure
   */
  @Override
  public void truncate(Zxid zxid) throws IOException {
    int lastKeepIdx = getFileIdx(zxid);
    for (int i = lastKeepIdx + 1; i < logFiles.size(); ++i) {
      // Deletes all the log files after the file which contains the
      // transaction with zxid.
      File file = logFiles.get(i);
      boolean result = file.delete();
      if (!result) {
        LOG.warn("The file {} might not be deleted successfully.",
                  file.getName());
      }
    }
    if (lastKeepIdx != -1) {
      File file = this.logFiles.get(lastKeepIdx);
      try (SimpleLog log = new SimpleLog(file)) {
        log.truncate(zxid);
      }
    }
    logFiles.subList(lastKeepIdx+ 1, logFiles.size()).clear();
    this.currentLog = getLastLog();
    this.lastSeenZxid = getLatestZxid();
  }

  /**
   * Gets the latest appended transaction id from the log.
   *
   * @return the transaction id of the latest transaction.
   * or Zxid.ZXID_NOT_EXIST if the log is empty.
   * @throws IOException in case of IO failure
   */
  @Override
  public Zxid getLatestZxid() throws IOException {
    if (logFiles.isEmpty()) {
      return Zxid.ZXID_NOT_EXIST;
    }
    try (SimpleLog log = getLastLog()) {
      return log.getLatestZxid();
    }
  }

  /**
   * Gets an iterator to read transactions from this log starting
   * at the given zxid (including zxid).
   *
   * @param zxid the id of the transaction.
   * @return an iterator to read the next transaction in logs.
   * @throws IOException in case of IO failure
   */
  @Override
  public LogIterator getIterator(Zxid zxid) throws IOException {
    return new RollingLogIterator(zxid);
  }

  /**
   * See {@link Log#firstDivergingPoint}.
   *
   * @param zxid the id of the transaction.
   * @return a tuple holds first diverging zxid and an iterator points to
   * subsequent transactions.
   * @throws IOException in case of IO failures
   */
  @Override
  public DivergingTuple firstDivergingPoint(Zxid zxid) throws IOException {
    int idx = getFileIdx(zxid);
    if (idx == -1) {
      Log.LogIterator iter = new RollingLogIterator(Zxid.ZXID_NOT_EXIST);
      return new DivergingTuple(iter, Zxid.ZXID_NOT_EXIST);
    }
    Zxid firstZxid = getZxidFromFileName(logFiles.get(idx));
    Log.LogIterator iter = new RollingLogIterator(firstZxid);
    Zxid prevZxid = firstZxid;
    while (iter.hasNext()) {
      Zxid curZxid = iter.next().getZxid();
      if (curZxid.compareTo(zxid) == 0) {
        return new DivergingTuple(iter, zxid);
      }
      if (curZxid.compareTo(zxid) > 0) {
        iter.close();
        return new DivergingTuple(new RollingLogIterator(curZxid), prevZxid);
      }
      prevZxid = curZxid;
    }
    return new DivergingTuple(iter, prevZxid);
  }

  /**
   * Syncs all the appended transactions to the physical media.
   *
   * @throws IOException in case of IO failure
   */
  @Override
  public void sync() throws IOException {
    if (this.currentLog != null) {
      this.currentLog.sync();
    }
  }

  /**
   * Trim the log up to the transaction with Zxid zxid inclusively.
   *
   * @param zxid the last zxid(inclusive) which will be trimed to.
   * @throws IOException in case of IO failures
   */
  @Override
  public void trim(Zxid zxid) throws IOException {
    throw new UnsupportedOperationException("Not supported");
  }

  // Initialize from the log directory.
  void initFromDir() {
    for (File file : this.logDir.listFiles()) {
      if (!file.isDirectory() &&
          file.getName().matches("transaction\\.\\d+_\\d+")) {
        // Appends the file with valid name to log file list.
        this.logFiles.add(file);
      }
    }
    if (!this.logFiles.isEmpty()) {
      // Sorts the file by the zxid order.
      Collections.sort(this.logFiles);
    }
  }

  /**
   * Given the zxid, find out the idx of the file in list which contains the
   * transaction with this zxid if and only if the transaction with the zxid
   * is in RollingLog. If the zxid is smaller than the smallest zxid in log,
   * -1 will be returned.
   *
   * @param zxid the zxid of the transaction.
   * @return the idx of file which possibly contains the transaction with
   * given zxid.
   */
  int getFileIdx(Zxid zxid) {
    if (logFiles.isEmpty() ||
        zxid.compareTo(getZxidFromFileName(logFiles.get(0))) < 0) {
      // If there's no log files or the zxid is smaller than the smallest zxid
      // of the rolling log, returns -1.
      return -1;
    }
    int idx = 0;
    while (idx < logFiles.size() - 1) {
      Zxid firstZxid = getZxidFromFileName(logFiles.get(idx));
      if (zxid.compareTo(firstZxid) == 0) {
        break;
      } else if (zxid.compareTo(firstZxid) > 0) {
        int nextIdx = idx + 1;
        if (nextIdx < logFiles.size()) {
          Zxid nextFirstZxid = getZxidFromFileName(logFiles.get(nextIdx));
          if (zxid.compareTo(nextFirstZxid) < 0) {
            // Means the zxid is larger than the smallest allowed zxid of log
            // file of index i but smaller than the smallest allowed zxid of
            // log file of index i + 1. So the transaction with zxid only
            // can be possibly in log file with idx i.
            break;
          }
        }
      }
      idx++;
    }
    return idx;
  }

  /**
   * Given the log file, finds out the smallest allowed zxid for thi file. It's
   * infered by looking at the name of the file.
   *
   * @return the smallest allowed zxid in this log file.
   */
  Zxid getZxidFromFileName(File file) {
    String fileName = file.getName();
    String strZxid = fileName.substring(fileName.indexOf('.') + 1);
    return Zxid.fromSimpleString(strZxid);
  }

  /**
   * Gets the last log file in the list of logs.
   *
   * @return the SimpleLog instance of the last log.
   */
  SimpleLog getLastLog() throws IOException {
    if (logFiles.isEmpty()) {
      return null;
    }
    return new SimpleLog(logFiles.get(logFiles.size() - 1));
  }

  /**
   * An implementation of LogIterator for RollingLog.
   */
  class RollingLogIterator implements Log.LogIterator {
    int fileIdx;
    Log.LogIterator iter;

    public RollingLogIterator(Zxid startZxid) throws IOException {
      int idx = getFileIdx(startZxid);
      if (logFiles.isEmpty()) {
        this.fileIdx = -1;
        this.iter = null;
      } else {
        if (idx == -1) {
          idx = 0;
        }
        this.fileIdx = idx;
        try (SimpleLog log = new SimpleLog(logFiles.get(this.fileIdx))) {
          this.iter = log.getIterator(startZxid);
        }
      }
    }

    /**
     * Closes the log file and release the resource.
     *
     * @throws IOException in case of IO failure
     */
    @Override
    public void close() throws IOException {
      if (this.iter != null) {
        this.iter.close();
      }
    }

    /**
     * Checks if it has more transactions.
     *
     * @return true if it has more transactions, false otherwise.
     */
    @Override
    public boolean hasNext() {
      return this.fileIdx != -1 &&
             (this.iter.hasNext() || this.fileIdx < (logFiles.size() - 1));
    }

    /**
     * Goes to the next transaction record.
     *
     * @return the next transaction record
     * @throws java.io.EOFException if it reaches the end of file before reading
     *                              the entire transaction.
     * @throws IOException in case of IO failure
     * @throws NoSuchElementException
     * if there's no more elements to get
     */
    @Override
    public Transaction next() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      if (!this.iter.hasNext()) {
        this.iter.close();
        this.fileIdx++;
        File nextFile = logFiles.get(this.fileIdx);
        this.iter = new SimpleLog.SimpleLogIterator(nextFile);
      }
      return this.iter.next();
    }
  }
}

