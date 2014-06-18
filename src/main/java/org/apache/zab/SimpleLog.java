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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the Log interface.
 * The format of a Transaction log is as follows:
 *
 * log-file := [ transactions ]
 *
 * transactions := transaction | transaction transactions
 *
 * transaction := zxid body-length transaction-body
 *
 * zxid := epoch(int) xid(int)
 *
 * body-length := length of transaction-body(int)
 *
 * transaction-body := byte array
 */
public class SimpleLog implements Log {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleLog.class);
  private final File logFile;
  private final DataOutputStream logStream;
  private final FileOutputStream fout;
  private Zxid lastZxidSeen = null;

  /**
   * Creates a transaction log. The logFile can be either
   * a new file or an existing file. If it's an existing
   * file, then the new log will be appended to the end of
   * the log.
   *
   * @param logFile the log file
   * @throws IOException in case of IO failure
   */
  public SimpleLog(File logFile) throws IOException {
    this.logFile = logFile;
    this.fout = new FileOutputStream(logFile, true);
    this.logStream = new DataOutputStream(
                     new BufferedOutputStream(fout));
    // Initializes the last seen zxid to the last zxid in file.
    this.lastZxidSeen = getLatestZxid();
  }

  /**
   * Closes the log file and release the resource.
   *
   * @throws IOException in case of IO failure
   */
  @Override
  public void close() throws IOException {
    this.logStream.close();
  }

  /**
   * Appends a request to transaction log.
   *
   * @param txn the transaction which will be added to log.
   * @throws IOException in case of IO failure
   */
  @Override
  public void append(Transaction txn) throws IOException {
    if(txn.getZxid().compareTo(this.lastZxidSeen) <= 0) {
      LOG.error("Cannot append {}. lastZxidSeen = {}",
                txn.getZxid(), this.lastZxidSeen);
      throw new RuntimeException("The id of the transaction is less "
          + "than the id of last seen transaction");
    }
    try {
      ByteBuffer buf = txn.getBody();
      this.logStream.writeInt(txn.getZxid().getEpoch());
      this.logStream.writeInt(txn.getZxid().getXid());
      this.logStream.writeInt(buf.remaining());
      // Write the data of ByteBuffer to stream.
      while (buf.hasRemaining()) {
        this.logStream.writeByte(buf.get());
      }
      this.logStream.flush();
      // Update last seen Zxid.
      this.lastZxidSeen = txn.getZxid();
    } catch(IOException e) {
      this.logStream.close();
    }
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
    SimpleLogIterator iter = new SimpleLogIterator(this.logFile);
    while(iter.hasNext()) {
      Transaction txn = iter.next();
      if(txn.getZxid().compareTo(zxid) >= 0) {
        break;
      }
    }
    if(iter.hasNext()) {
      // It means there's something to truncate.
      RandomAccessFile ra = new RandomAccessFile(this.logFile, "rw");
      try {
        // Truncate the file from given position.
        ra.setLength(iter.getPosition());
      } finally {
        ra.close();
        iter.close();
      }
    }
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
    Log.LogIterator iter = new SimpleLogIterator(this.logFile);
    Transaction txn = null;
    try {
      while (iter.hasNext()) {
        txn = iter.next();
      }
      if(txn == null) {
        return Zxid.ZXID_NOT_EXIST;
      }
      return txn.getZxid();
    } finally {
      iter.close();
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
    SimpleLogIterator iter = new SimpleLogIterator(this.logFile);
    while(iter.hasNext()) {
      Transaction txn = iter.next();
      // Break if we find one record which has equal or higher transaction id.
      if(txn.getZxid().compareTo(zxid) > 0) {
        break;
      } else if(txn.getZxid().compareTo(zxid) == 0) {
        // Reset the iterator points to beginning of the transaction.
        iter.backward();
        break;
      }
    }
    return iter;
  }

  /**
   * Syncs all the appended transactions to the physical media.
   *
   * @throws IOException in case of IO failure
   */
  @Override
  public void sync() throws IOException {
    this.logStream.flush();
    this.fout.getChannel().force(false);
  }

  /**
   * An implementation of iterator for iterating the log.
   */
  public static class SimpleLogIterator implements Log.LogIterator {
    private final DataInputStream logStream;
    private final FileInputStream fin;
    private final File logFile;
    private int position = 0;
    private int lastTransactionLength = 0;

    public SimpleLogIterator(File logFile) throws IOException {
      this.logFile = logFile;
      this.fin = new FileInputStream(logFile);
      this.logStream = new DataInputStream(this.fin);
    }

    /**
     * Gets the position of this iterator in file.
     * @return the position in file
     */
    public int getPosition() {
      return this.position;
    }

    /**
     * Closes the log file and release the resource.
     *
     * @throws IOException in case of IO failure
     */
    @Override
    public void close() throws IOException {
      this.logStream.close();
    }

    /**
     * Checks if it has more transactions.
     *
     * @return true if it has more transactions, false otherwise.
     */
    @Override
    public boolean hasNext() {
      return this.position < this.logFile.length();
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
      if(!hasNext()) {
        throw new NoSuchElementException();
      }

      DataInputStream in = new DataInputStream(logStream);
      int epoch, xid;
      epoch = in.readInt();
      xid = in.readInt();
      Zxid zxid = new Zxid(epoch, xid);
      // Reads the length of the transaction body.
      int bodyLength = in.readInt();
      byte[] bodyBuffer = new byte[bodyLength];
      // Reads the data of the transaction body.
      in.readFully(bodyBuffer, 0, bodyLength);
      this.lastTransactionLength = Zxid.getZxidLength() + 4 + bodyLength;
      // Updates the position of file.
      this.position += this.lastTransactionLength;
      return new Transaction(zxid, ByteBuffer.wrap(bodyBuffer));
    }

    // Moves the transaction log backward to last transaction.
    private void backward() throws IOException {
      this.position -= this.lastTransactionLength;
      this.fin.getChannel().position(this.position);
      this.lastTransactionLength = 0;
    }
  }
}
