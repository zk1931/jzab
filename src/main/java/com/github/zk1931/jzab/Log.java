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

/**
 *  Interface of manipulating transaction log.
 */
interface Log extends AutoCloseable {
  /**
   * Appends a request to transaction log.
   *
   * @param txn the transaction which will be appended to log.
   * @throws IOException in case of IO failures
   */
  void append(Transaction txn) throws IOException;

  /**
   * Truncates this transaction log at the given zxid.
   *
   * This method deletes all the transactions with zxids
   * higher than the given zxid.
   *
   * @param zxid the transaction id.
   * @throws IOException in case of IO failures
   */
  void truncate(Zxid zxid) throws IOException;

  /**
   * Gets the latest transaction id from log.
   *
   * @return the transaction id of the latest transaction.
   * Return Zxid.ZXID_NOT_EXIST if the log is empty.
   * @throws IOException in case of IO failures
   */
  Zxid getLatestZxid() throws IOException;

  /**
   * Gets an iterator to read transactions from this log starting
   * at the given zxid (including zxid).
   *
   * @param zxid the id of the transaction.
   * @return an iterator to read the next transaction in logs.
   * @throws IOException in case of IO failures
   */
  LogIterator getIterator(Zxid zxid) throws IOException;

  /**
   * Given a zxid, finds out the first diverging point and returns a tuple
   * of (diverging zxid, iterator points to next txn after diverging zxid).
   * If there's no diverging point, which means the zxid is a prefix of the
   * server's log, then the diverging zxid will be as same as giving zxid.
   *
   * Examples:
   *
   * <pre>
   * case 1:
   *  the log : (0, 0), (0, 1), (1, 1)
   *  zxid : (0, 2)
   *  returns ((0, 1), iter points to (1, 1))
   *
   * case 2:
   *  the log : (0, 0), (0, 1), (1, 1)
   *  zxid : (0, 1)
   *  returns ((0, 1), iter points to (1, 1))
   *
   * case 3:
   *  the log : (0, 0), (0, 1), (1, 1)
   *  zxid : (1, 2)
   *  returns ((1, 1), iter points to end of the log)
   *
   * case 4:
   *  the log : (0, 2)
   *  zxid : (0, 1)
   *  returns ((0, -1), iter points (0, 2))
   *</pre>
   *
   * @param zxid the id of the transaction.
   * @return a tuple holds first diverging zxid and an iterator points to
   * subsequent transactions.
   * @throws IOException in case of IO failures
   */
  DivergingTuple firstDivergingPoint(Zxid zxid) throws IOException;

  /**
   * Syncs all the appended transactions to the physical media.
   *
   * @throws IOException in case of IO failures
   */
  void sync() throws IOException;

  /**
   * Trim the log up to the transaction of zxid inclusively.
   *
   * @param zxid the last zxid(inclusive) which will be trimed to.
   * @throws IOException in case of IO failures
   */
  void trim(Zxid zxid) throws IOException;

  /**
   * Closes the log file and release the resource.
   *
   * @throws IOException in case of IO failures
   */
  @Override
  void close() throws IOException;

  /**
   *  Interface of iterating the transaction log.
   */
  public interface LogIterator extends AutoCloseable {
    /**
     * Checks if it has next transaction record.
     *
     * @return true if it has more transactions, false otherwise.
     */
    boolean hasNext();

    /**
     * Goes to the next transaction.
     *
     * @return the next transaction record
     * @throws IOException in case of IO failures
     * @throws java.util.NoSuchElementException
     * if there's no more element to get
     */
    Transaction next() throws IOException;

    /**
     * Closes the iterator and releases its underlying resource.
     *
     * @throws IOException in case of IO failures
     */
    @Override
    void close() throws IOException;
  }

  /**
   * The tuple holds the return value of firstDivergingPoint function.
   */
  static class DivergingTuple {
    private final LogIterator iter;
    private final Zxid zxid;

    public DivergingTuple(LogIterator iter, Zxid zxid) {
      this.iter = iter;
      this.zxid = zxid;
    }

    public LogIterator getIterator() {
      return this.iter;
    }

    public Zxid getDivergingZxid() {
      return this.zxid;
    }
  }
}
