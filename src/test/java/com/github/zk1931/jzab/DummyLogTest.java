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
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the dummy log.
 */
public class DummyLogTest extends TestBase {
  @Test
  public void testAppend() throws IOException {
    DummyLog log = new DummyLog(4);
    Log.LogIterator iter = log.getIterator(new Zxid(0, 1));
    Transaction txn = iter.next();
    Assert.assertEquals(txn.getZxid(), new Zxid(0, 1));
    Assert.assertTrue(txn.getBody()
                      .equals(ByteBuffer.wrap("txn1".getBytes())));

    txn = iter.next();
    Assert.assertEquals(txn.getZxid(), new Zxid(0, 2));
    Assert.assertTrue(txn.getBody()
                      .equals(ByteBuffer.wrap("txn2".getBytes())));

    txn = iter.next();
    Assert.assertEquals(txn.getZxid(), new Zxid(0, 3));
    Assert.assertTrue(txn.getBody()
                      .equals(ByteBuffer.wrap("txn3".getBytes())));

    // It should be the end of the log file.
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testGetLatestZxid() throws IOException {
    DummyLog log = new DummyLog(4);
    Assert.assertEquals(log.getLatestZxid(), new Zxid(0, 3));
  }

  @Test
  public void testTruncateFile() throws IOException {
    DummyLog log = new DummyLog(4);
    log.truncate(new Zxid(0, 1));
    Assert.assertEquals(log.getLatestZxid(), new Zxid(0, 1));
  }

  @Test
  public void testTruncateAndAppend() throws IOException {
    DummyLog log = new DummyLog(4);
    log.truncate(new Zxid(0, 0));
    log.append(new Transaction(new Zxid(1, 2),
                               ByteBuffer.wrap("txn1.2".getBytes())));
    Log.LogIterator iter = log.getIterator(new Zxid(0, 0));
    Assert.assertEquals(iter.next().getZxid(), new Zxid(0, 0));
    Assert.assertEquals(iter.next().getZxid(), new Zxid(1, 2));
  }

  /**
   * Appending a transaction with a zxid smaller than the previous zxid should
   * result in a RuntimeException.
   */
  @Test(expected=RuntimeException.class)
  public void testAppendSmallerZxid() throws IOException {
    DummyLog log = new DummyLog();
    log.append(new Transaction(new Zxid(0, 1),
                               ByteBuffer.wrap("log record 1".getBytes())));
    log.append(new Transaction(new Zxid(0, 0),
                               ByteBuffer.wrap("log record 0".getBytes())));
  }

  /**
   * Appending a transaction with a zxid equal to the previous zxid should
   * result in a RuntimeException.
   */
  @Test(expected=RuntimeException.class)
  public void testAppendSameZxid() throws IOException {
    DummyLog log = new DummyLog();
    log.append(new Transaction(new Zxid(0, 0),
                               ByteBuffer.wrap("log record 0".getBytes())));
    log.append(new Transaction(new Zxid(0, 0),
                               ByteBuffer.wrap("log record 0".getBytes())));
  }

  /**
   * Test writing byte buffer with nonzero position.
   */
  @Test
  public void testWriteNonZeroBuffer() throws IOException {
    DummyLog log = new DummyLog();
    ByteBuffer buf = ByteBuffer.allocate("Hello World".getBytes().length);
    buf.put("Hello World".getBytes());
    buf.flip();
    // Make it position non-zero.
    int h = buf.get();
    int e = buf.get();
    Assert.assertEquals(h, 'H');
    Assert.assertEquals(e, 'e');
    Assert.assertEquals(buf.position(), 2);
    Transaction t = new Transaction(new Zxid(0, 0), buf);
    log.append(t);

    Log.LogIterator iter = log.getIterator(new Zxid(0, 0));
    Transaction txn = iter.next();
    Assert.assertEquals(txn.getZxid(), new Zxid(0, 0));
    // Check the first two characters are not in log.
    Assert.assertTrue(txn.getBody().
                      equals(ByteBuffer.wrap("llo World".getBytes())));
  }

  /**
   * Tests whether the getIterator method works as our expectation.
   *
   * @throws IOException in case of IO failure.
   */
  @Test
  public void testIterator() throws IOException {
    DummyLog log = new DummyLog();
    log.append(new Transaction(new Zxid(0, 1),
                               ByteBuffer.wrap("log record 1".getBytes())));

    Log.LogIterator iter = log.getIterator(new Zxid(0, 0));
    Assert.assertEquals(iter.next().getZxid(), new Zxid(0, 1));
  }
}

