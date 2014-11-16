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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for different log implementations.
 */
@RunWith(Parameterized.class)
public class LogTest extends TestBase {

  private static final Logger LOG = LoggerFactory.getLogger(LogTest.class);

  Class<Log> logClass;

  String fileName;

  @Parameterized.Parameters
  public static Collection<Object[]> instancesToTest() throws Exception {
    return Arrays.asList(
      new Object[][] {
        {SimpleLog.class, "transaction.log"},
        {RollingLog.class, ""}
      });
  }

  public LogTest(Class<Log> logClass, String fileName) {
    LOG.debug("Testting impelementation of {}", logClass);
    this.logClass = logClass;
    this.fileName = fileName;
  }

  Log getLog() throws Exception {
    File f = new File(getDirectory(), this.fileName);
    if (logClass == (Class<?>)RollingLog.class) {
      // For testing purpose, set the rolling size to be very small.
      return logClass.getConstructor(File.class, Long.TYPE)
                          .newInstance(f, 128);
    } else {
      return logClass.getConstructor(File.class).newInstance(f);
    }
  }

  void corruptFile(int position) throws Exception {
    File file = new File(getDirectory(), "transaction.log");
    RandomAccessFile ra = new RandomAccessFile(file, "rw");
    ra.seek(position);
    ra.write(0xff);
    ra.close();
  }

  /**
   * Gets transaction id of all the transactions after zxid in log.
   */
  List<Zxid> getZxids(Log log, Zxid startZxid) throws IOException {
    List<Zxid> zxids = new ArrayList<Zxid>();
    try (Log.LogIterator iter = log.getIterator(startZxid)) {
      while (iter.hasNext()) {
        Zxid zxid = iter.next().getZxid();
        zxids.add(zxid);
      }
    }
    return zxids;
  }

  @Test
  public void testAppend() throws Exception {
    Log log = getLog();
    // Appends  100 transactions.
    appendTxns(log, new Zxid(0, 0), 100);
    List<Zxid> zxids = getZxids(log, Zxid.ZXID_NOT_EXIST);
    // Check all the transactions are in log.
    Assert.assertEquals(100, zxids.size());
    // Check if the transactions are correct.
    Assert.assertEquals(new Zxid(0, 0), zxids.get(0));
    Assert.assertEquals(new Zxid(0, zxids.size() - 1),
                        zxids.get(zxids.size() - 1));
  }

  @Test
  public void testGetLatestZxid() throws Exception {
    Log log = getLog();
    // Appends  100 transactions.
    appendTxns(log, new Zxid(0, 0), 100);
    // Make sure latest zxid is <0, 99>.
    Assert.assertEquals(new Zxid(0, 99), log.getLatestZxid());
  }

  @Test
  public void testTruncateFile() throws Exception {
    Log log = getLog();
    // Appends  100 transactions.
    appendTxns(log, new Zxid(0, 0), 100);
    // Make sure latest zxid is <0, 99>.
    Assert.assertEquals(new Zxid(0, 99), log.getLatestZxid());
    int truncIdx = new Random().nextInt(100);
    log.truncate(new Zxid(0, truncIdx));
    // Make sure latest zxid is <0, truncIdx>.
    Assert.assertEquals(new Zxid(0, truncIdx), log.getLatestZxid());
  }

  @Test
  public void testTruncateAll() throws Exception {
    Log log = getLog();
    // Appends  100 transactions.
    appendTxns(log, new Zxid(0, 0), 100);
    log.truncate(new Zxid(0, -1));
    appendTxns(log, new Zxid(0, 0), 100);
  }

  @Test
  public void testTruncateAndAppend() throws Exception {
    Log log = getLog();
    // Appends  100 transactions.
    appendTxns(log, new Zxid(0, 0), 100);
    // Make sure latest zxid is <0, 99>.
    Assert.assertEquals(new Zxid(0, 99), log.getLatestZxid());
    // Truncates file after <0, 77>
    log.truncate(new Zxid(0, 77));
    // Make sure latest zxid is <0, 77>.
    Assert.assertEquals(new Zxid(0, 77), log.getLatestZxid());
    // Appends 22 more transactions.
    appendTxns(log, new Zxid(0, 78), 22);
    List<Zxid> zxids = getZxids(log, Zxid.ZXID_NOT_EXIST);
    // Make sure latest zxid is <0, 99>
    Assert.assertEquals(new Zxid(0, 99), log.getLatestZxid());
    // Check all the transactions are in log.
    Assert.assertEquals(100, zxids.size());
  }

  /**
   * Appending a transaction with a zxid smaller than the previous zxid should
   * result in a RuntimeException.
   */
  @Test(expected=RuntimeException.class)
  public void testAppendSmallerZxid() throws Exception {
    Log log = getLog();
    log.append(new Transaction(new Zxid(0, 1),
                               ByteBuffer.wrap("txn".getBytes())));
    log.append(new Transaction(new Zxid(0, 0),
                               ByteBuffer.wrap("txn".getBytes())));
  }

  /**
   * Appending a transaction with the zxid equal to previous zxid should
   * result in a RuntimeException.
   */
  @Test(expected=RuntimeException.class)
  public void testAppendSameZxid() throws Exception {
    Log log = getLog();
    log.append(new Transaction(new Zxid(0, 1),
                               ByteBuffer.wrap("txn".getBytes())));
    log.append(new Transaction(new Zxid(0, 1),
                               ByteBuffer.wrap("txn".getBytes())));
  }

  /**
   * Tests whether the getIterator method works as our expectation.
   */
  @Test
  public void testIterator() throws Exception {
    Log log = getLog();
    // Appends  100 transactions.
    appendTxns(log, new Zxid(0, 0), 100);
    Random rand = new Random();
    Zxid begZxid = new Zxid(0, rand.nextInt(100));
    Log.LogIterator iter = log.getIterator(begZxid);
    // Make sure it starts from the correct zxid.
    Assert.assertEquals(begZxid, iter.next().getZxid());
    // Starts from negative zxid.
    iter = log.getIterator(new Zxid(0, -1));
    Assert.assertEquals(new Zxid(0, 0), iter.next().getZxid());
  }

  @Test
  public void testRecoverFromExistingLog() throws Exception {
    Log log = getLog();
    // Appends  100 transactions.
    appendTxns(log, new Zxid(0, 0), 100);
    log.close();
    // Reopen the log.
    log = getLog();
    List<Zxid> zxids = getZxids(log, Zxid.ZXID_NOT_EXIST);
    // Check all the transactions are still in log.
    Assert.assertEquals(100, zxids.size());
    // Check all the transactions are in correct order.
    for (int i = 0; i < zxids.size(); ++i) {
      Zxid zxid = new Zxid(0, i);
      Assert.assertEquals(zxid, zxids.get(i));
    }
  }

  @Test(expected=RuntimeException.class)
  public void testCorruptChecksum() throws Exception {
    if (logClass == (Class<?>)SimpleLog.class) {
      Log log = getLog();
      appendTxns(log, new Zxid(0, 0), 1);
      // Corrupts checksum.
      corruptFile(0);
      // Iterating the log will trigger the checksum error.
      log.getIterator(log.getLatestZxid());
      log.close();
    } else {
      throw new RuntimeException("Simulated exception for RollingLog.");
    }
  }

  @Test(expected=RuntimeException.class)
  public void testCorruptLength() throws Exception {
    if (logClass == (Class<?>)SimpleLog.class) {
      Log log = getLog();
      appendTxns(log, new Zxid(0, 0), 1);
      // Corrupts length.
      corruptFile(4);
      // Iterating the log will trigger the checksum error.
      log.getIterator(log.getLatestZxid());
      log.close();
    } else {
      throw new RuntimeException("Simulated exception for RollingLog.");
    }
  }

  @Test(expected=RuntimeException.class)
  public void testCorruptZxid() throws Exception {
    if (logClass == (Class<?>)SimpleLog.class) {
      Log log = getLog();
      appendTxns(log, new Zxid(0, 0), 1);
      // Corrupts zxid.
      corruptFile(8);
      // Iterating the log will trigger the checksum error.
      log.getIterator(log.getLatestZxid());
      log.close();
    } else {
      throw new RuntimeException("Simulated exception for RollingLog.");
    }
  }

  @Test(expected=RuntimeException.class)
  public void testCorruptType() throws Exception {
    if (logClass == (Class<?>)SimpleLog.class) {
      Log log = getLog();
      appendTxns(log, new Zxid(0, 0), 1);
      // Corrupts type.
      corruptFile(16);
      // Iterating the log will trigger the checksum error.
      log.getIterator(log.getLatestZxid());
      log.close();
    } else {
      throw new RuntimeException("Simulated exception for RollingLog.");
    }
  }

  @Test(expected=RuntimeException.class)
  public void testCorruptTxn() throws Exception {
    if (logClass == (Class<?>)SimpleLog.class) {
      Log log = getLog();
      appendTxns(log, new Zxid(0, 0), 1);
      // Corrupts Transaction.
      corruptFile(20);
      // Iterating the log will trigger the checksum error.
      log.getIterator(log.getLatestZxid());
      log.close();
    } else {
      throw new RuntimeException("Simulated exception for RollingLog.");
    }
  }

  @Test
  public void testDivergingPoint() throws Exception {
    /*
     * case 1:
     *  the log : <0, 0>, <0, 1>, <1, 1>
     *  zxid : <0, 2>
     *  returns (<0, 1>, iter points to <1, 1>)
     */
    Log log = getLog();
    appendTxns(log, new Zxid(0, 0), 2);
    appendTxns(log, new Zxid(1, 1), 1);
    Log.DivergingTuple dp = log.firstDivergingPoint(new Zxid(0, 2));
    Assert.assertEquals(new Zxid(0, 1), dp.getDivergingZxid());
    Assert.assertEquals(new Zxid(1, 1), dp.getIterator().next().getZxid());

    /*
     * case 2:
     *  the log : <0, 0>, <0, 1>, <1, 1>
     *  zxid : <0, 1>
     *  returns (<0, 1>, iter points to <1, 1>)
     */
    dp = log.firstDivergingPoint(new Zxid(0, 1));
    Assert.assertEquals(new Zxid(0, 1), dp.getDivergingZxid());
    Assert.assertEquals(new Zxid(1, 1), dp.getIterator().next().getZxid());

    /*
     * case 3:
     *  the log : <0, 0>, <0, 1>, <1, 1>
     *  zxid : <1, 2>
     *  returns (<1, 1>, iter points to end of the log)
     */
    dp = log.firstDivergingPoint(new Zxid(1, 2));
    Assert.assertEquals(new Zxid(1, 1), dp.getDivergingZxid());
    Assert.assertFalse(dp.getIterator().hasNext());

    /*
     * case 4:
     *  the log : <0, 2>
     *  zxid : <0, 1>
     *  returns (<0, -1>, iter points <0, 2>)
     */
    log.truncate(Zxid.ZXID_NOT_EXIST);
    appendTxns(log, new Zxid(0, 2), 1);
    dp = log.firstDivergingPoint(new Zxid(0, 1));
    Assert.assertEquals(Zxid.ZXID_NOT_EXIST, dp.getDivergingZxid());
    Assert.assertEquals(new Zxid(0, 2), dp.getIterator().next().getZxid());

    /*
     * case 5:
     *  the log : <0, 2>
     *  zxid : <0, -1>
     *  returns (<0, -1>, iter points <0, 2>)
     */
    dp = log.firstDivergingPoint(Zxid.ZXID_NOT_EXIST);
    Assert.assertEquals(Zxid.ZXID_NOT_EXIST, dp.getDivergingZxid());
    Assert.assertEquals(new Zxid(0, 2), dp.getIterator().next().getZxid());

    /*
     * case 6:
     *  the log : empty
     *  zxid : <0, 1>
     *  returns (<0, -1>, iter points to end of the log)
     */
    log.truncate(Zxid.ZXID_NOT_EXIST);
    dp = log.firstDivergingPoint(new Zxid(0, 1));
    Assert.assertEquals(Zxid.ZXID_NOT_EXIST, dp.getDivergingZxid());
    Assert.assertFalse(dp.getIterator().hasNext());
  }

  @Test
  public void testSyncEmptyLog() throws Exception {
    Log log = getLog();
    log.sync();
    log.close();
  }
}
