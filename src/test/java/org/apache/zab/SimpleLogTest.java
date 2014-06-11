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
import java.io.IOException;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test the simple implementation of transaction log.
 */
public class SimpleLogTest {
  private static final String LOGFILE= "LogTest";

  private SimpleLog initLog() throws IOException {
    File temp = File.createTempFile(LOGFILE, "tmp");
    SimpleLog log = new SimpleLog(temp);
    log.append(new Transaction(new Zxid(0, 0), "log record 1".getBytes()));
    log.append(new Transaction(new Zxid(0, 1), "log record 2".getBytes()));
    log.append(new Transaction(new Zxid(0, 2), "log record 12".getBytes()));
    log.append(new Transaction(new Zxid(0, 3), "log record 13".getBytes()));
    return log;
  }

  @Test
  public void testAppend() throws IOException {
    SimpleLog log = initLog();
    Log.LogIterator iter = log.getIterator(new Zxid(0, 1));
    Transaction txn = iter.next();
    Assert.assertEquals(txn.getZxid(), new Zxid(0, 1));
    Assert.assertTrue(Arrays.equals(txn.getBody(), "log record 2".getBytes()));

    txn = iter.next();
    Assert.assertEquals(txn.getZxid(), new Zxid(0, 2));
    Assert.assertTrue(Arrays.equals(txn.getBody(), "log record 12".getBytes()));

    txn = iter.next();
    Assert.assertEquals(txn.getZxid(), new Zxid(0, 3));
    Assert.assertTrue(Arrays.equals(txn.getBody(), "log record 13".getBytes()));
    // It should be the end of the log file.
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testGetLatestZxid() throws IOException {
    SimpleLog log = initLog();
    Assert.assertEquals(log.getLatestZxid(), new Zxid(0, 3));
  }

  @Test
  public void testTruncateFile() throws IOException {
    SimpleLog log = initLog();
    log.truncate(new Zxid(0, 1));
    Assert.assertEquals(log.getLatestZxid(), new Zxid(0, 1));
  }

  @Test
  public void testReopenFile() throws IOException {
    File temp = File.createTempFile(LOGFILE, "tmp");
    SimpleLog log = new SimpleLog(temp);
    log.append(new Transaction(new Zxid(0, 0), "log record 1".getBytes()));
    log.close();
    log = new SimpleLog(temp);

    log.append(new Transaction(new Zxid(0, 1), "log record 2".getBytes()));
    Log.LogIterator iter = log.getIterator(new Zxid(0, 0));
    Assert.assertEquals(iter.next().getZxid(), new Zxid(0, 0));
    Assert.assertEquals(iter.next().getZxid(), new Zxid(0, 1));
  }

  @Test
  public void testTruncateAndAppend() throws IOException {
    SimpleLog log = initLog();
    log.truncate(new Zxid(0, 0));
    log.append(new Transaction(new Zxid(1, 2), "log record 1 2".getBytes()));
    Log.LogIterator iter = log.getIterator(new Zxid(0, 0));
    Assert.assertEquals(iter.next().getZxid(), new Zxid(0, 0));
    Assert.assertEquals(iter.next().getZxid(), new Zxid(1, 2));
  }

  @Test
  public void testAppendWithoutSync() throws IOException {
    File temp = File.createTempFile(LOGFILE, "tmp");
    SimpleLog log = new SimpleLog(temp);
    log.append(new Transaction(new Zxid(0, 0), "log record 1".getBytes()));
    Log.LogIterator iter = log.getIterator(new Zxid(0, 0));
    Assert.assertEquals(iter.next().getZxid(), new Zxid(0, 0));
  }
}
