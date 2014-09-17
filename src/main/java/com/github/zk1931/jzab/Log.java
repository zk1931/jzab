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
public interface Log extends AutoCloseable {
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
   * @param zxid the id of the transaction. If zxid id null,
   * return the iterator from beginning of the log.
   * @return an iterator to read the next transaction in logs.
   * @throws IOException in case of IO failures
   */
  LogIterator getIterator(Zxid zxid) throws IOException;

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
}
