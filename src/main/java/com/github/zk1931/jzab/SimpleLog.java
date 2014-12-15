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
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.zip.Adler32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the Log interface.
 * The format of a Transaction log is as follows:
 *
 * <p>
 * <pre>
 * log-file     := [ transactions ]
 *
 * transactions := transaction | transaction transactions
 *
 * transaction  := checksum length zxid type payload
 *
 * checksum     := checksum(int)
 *
 * length       := length of zxid + type + payload
 *
 * zxid         := epoch(long) xid(long)
 *
 * type         := type(int)
 *
 * payload      := byte array
 * </pre>
 */
class SimpleLog implements Log {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleLog.class);
  private final File logFile;
  private final DataOutputStream logStream;
  private final FileOutputStream fout;
  private Zxid lastSeenZxid = null;

  // The number of bytes for Zxid field of transaction.
  private static final int ZXID_LENGTH = 16;

  // The number of bytes for type field of transaction.
  private static final int TYPE_LENGTH = 4;

  // The number of bytes for checksum field of transaction.
  private static final int CHECKSUM_LENGTH = 4;

  // The number of bytes for length field of transaction.
  private static final int LENGTH_LENGTH = 4;

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
    this.lastSeenZxid = getLatestZxid();
    LOG.debug("SimpleLog constructed. The lastSeenZxid is {}.",
              this.lastSeenZxid);
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
    if(txn.getZxid().compareTo(this.lastSeenZxid) <= 0) {
      LOG.error("Cannot append {}. lastSeenZxid = {}",
                txn.getZxid(), this.lastSeenZxid);
      throw new RuntimeException("The id of the transaction is less "
          + "than the id of last seen transaction");
    }
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      DataOutputStream dout = new DataOutputStream(bout);
      ByteBuffer payload = txn.getBody();
      // The number of bytes for Zxid + Type + payload.
      int length = payload.remaining() + ZXID_LENGTH + TYPE_LENGTH;
      // Writes the length.
      dout.writeInt(length);
      // Writes Zxid.
      dout.writeLong(txn.getZxid().getEpoch());
      dout.writeLong(txn.getZxid().getXid());
      // Writes the type of the transaction.
      dout.writeInt(txn.getType());
      // Writes the body of the transaction.
      while (payload.hasRemaining()) {
        dout.writeByte(payload.get());
      }
      dout.flush();
      byte[] blob = bout.toByteArray();
      // Calculates the checksum.
      Adler32 checksum = new Adler32();
      checksum.update(blob);
      // Gets the checksum value.
      int checksumValue = (int)checksum.getValue();
      this.logStream.writeInt(checksumValue);
      this.logStream.write(blob);
      this.logStream.flush();
      // Update last seen Zxid.
      this.lastSeenZxid = txn.getZxid();
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
    try (SimpleLogIterator iter = new SimpleLogIterator(this.logFile)) {
      this.lastSeenZxid = Zxid.ZXID_NOT_EXIST;
      while (iter.hasNext()) {
        Transaction txn = iter.next();
        if (txn.getZxid().compareTo(zxid) == 0) {
          this.lastSeenZxid = txn.getZxid();
          break;
        }
        if (txn.getZxid().compareTo(zxid) > 0) {
          iter.backward();
          break;
        }
        this.lastSeenZxid = txn.getZxid();
      }
      if (iter.hasNext()) {
        // It means there's something to truncate.
        try (RandomAccessFile ra = new RandomAccessFile(this.logFile, "rw")) {
          // Truncate the file from given position.
          ra.setLength(iter.getPosition());
        }
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
    if (this.lastSeenZxid != null) {
      // If the lastSeenZxid is cached, returns it directly.
      return this.lastSeenZxid;
    }
    Transaction txn = null;
    try (LogIterator iter = new SimpleLogIterator(this.logFile)) {
      while (iter.hasNext()) {
        txn = iter.next();
      }
      if (txn == null) {
        return Zxid.ZXID_NOT_EXIST;
      }
      return txn.getZxid();
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
      if(txn.getZxid().compareTo(zxid) >= 0) {
        iter.backward();
        break;
      }
    }
    return iter;
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
    SimpleLogIterator iter =
      (SimpleLogIterator)getIterator(Zxid.ZXID_NOT_EXIST);
    Zxid prevZxid = Zxid.ZXID_NOT_EXIST;
    while (iter.hasNext()) {
      Zxid curZxid = iter.next().getZxid();
      if (curZxid.compareTo(zxid) == 0) {
        return new DivergingTuple(iter, zxid);
      }
      if (curZxid.compareTo(zxid) > 0) {
        iter.backward();
        return new DivergingTuple(iter, prevZxid);
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
    this.logStream.flush();
    this.fout.getChannel().force(false);
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

  long length() {
    return this.logFile.length();
  }

  String getName() {
    return this.logFile.getName();
  }

  /**
   * An implementation of iterator for iterating the log.
   */
  public static class SimpleLogIterator implements Log.LogIterator {
    private DataInputStream logStream;
    private final FileInputStream fin;
    private final File logFile;
    private int position = 0;
    private int lastTransactionLength = 0;
    private Zxid prevZxid = Zxid.ZXID_NOT_EXIST;

    public SimpleLogIterator(File logFile) throws IOException {
      this.logFile = logFile;
      this.fin = new FileInputStream(logFile);
      this.logStream = new DataInputStream(new BufferedInputStream(this.fin));
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
      if (in.available() < CHECKSUM_LENGTH + LENGTH_LENGTH) {
        LOG.error("Not enough bytes for checksum field and length field.");
        throw new RuntimeException("Corrupted file.");
      }
      // Gets the checksum value.
      int checksumValue = in.readInt();
      int length = in.readInt();
      long epoch, xid;
      int type;
      if (length < ZXID_LENGTH + TYPE_LENGTH) {
        LOG.error("The length field is invalid. Previous txn is {}", prevZxid);
        throw new RuntimeException("The length field is invalid.");
      }
      byte[] rest = new byte[length];
      in.readFully(rest, 0, length);
      byte[] blob = ByteBuffer.allocate(length + LENGTH_LENGTH).putInt(length)
                                                               .put(rest)
                                                               .array();
      // Caculates the checksum.
      Adler32 checksum = new Adler32();
      checksum.update(blob);
      if ((int)checksum.getValue() != checksumValue) {
        String exStr =
          String.format("Checksum after txn %s mismathes in file %s, file "
                        + "corrupted?",
                        prevZxid, logFile.getName());
        LOG.error(exStr);
        throw new RuntimeException(exStr);
      }
      // Checksum is correct, parse the byte array.
      ByteArrayInputStream bin = new ByteArrayInputStream(rest);
      DataInputStream din = new DataInputStream(bin);
      // Reads the Zxid.
      epoch = din.readLong();
      xid = din.readLong();
      // Reads the type of transaction.
      type = din.readInt();
      int payloadLength = length - ZXID_LENGTH - TYPE_LENGTH;
      byte[] payload = new byte[payloadLength];
      // Reads the data of the transaction body.
      din.readFully(payload, 0, payloadLength);
      din.close();
      Zxid zxid = new Zxid(epoch, xid);
      this.prevZxid = zxid;
      this.lastTransactionLength = CHECKSUM_LENGTH + LENGTH_LENGTH + length;
      // Updates the position of file.
      this.position += this.lastTransactionLength;
      return new Transaction(zxid, type, ByteBuffer.wrap(payload));
    }

    // Moves the transaction log backward to last transaction.
    void backward() throws IOException {
      this.position -= this.lastTransactionLength;
      this.fin.getChannel().position(this.position);
      // Since we moved the file pointer, the buffered data should be
      // invalidated.
      this.logStream = new DataInputStream(new BufferedInputStream(this.fin));
      this.lastTransactionLength = 0;
    }
  }
}
