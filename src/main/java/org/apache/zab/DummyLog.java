/**
 * Licensed to the Apache Software Foundatlion (ASF) under one
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.ListIterator;

/**
 * Mock in-memory log, used for testing purpose only. (See documents of
 * Log interface)
 */
public class DummyLog implements Log {

  private final ArrayList<Transaction> memLog = new ArrayList<Transaction>();


  public DummyLog() {
  }

  public DummyLog(int count) {
    for (int i = 0; i < count; ++i) {
      Zxid zxid = new Zxid(0, i);
      ByteBuffer body = ByteBuffer.wrap(("txn" + i)
                        .getBytes(Charset.forName("UTF-8")));

      memLog.add(new Transaction(zxid, body));
    }
  }

  @Override
  public void append(Transaction txn) {
    if (txn.getZxid().compareTo(getLatestZxid()) <= 0) {
      throw new RuntimeException("The zxid of txn is smaller than last zxid"
          + "in log");
    }
    memLog.add(txn);
  }

  @Override
  public void truncate(Zxid zxid) {
    int idx;
    for (idx = 0; idx < memLog.size(); ++idx) {
      if (memLog.get(idx).getZxid().compareTo(zxid) > 0) {
        break;
      }
    }
    memLog.subList(idx, memLog.size()).clear();
  }

  @Override
  public Zxid getLatestZxid() {
    if (memLog.size() == 0) {
      return Zxid.ZXID_NOT_EXIST;
    }
    return memLog.get(memLog.size() - 1).getZxid();
  }

  @Override
  public LogIterator getIterator(Zxid zxid) throws IOException {
    DummyLogIterator iter = new DummyLogIterator();
    while (iter.hasNext()) {
      Zxid z = iter.next().getZxid();
      if (z.compareTo(zxid) >= 0) {
        // Backtrack one step.
        iter.iter.previous();
        break;
      }
    }
    return iter;
  }

  @Override
  public void sync() {
    // Do nothing.
  }

  @Override
  public void close() {
    // Do nothing.
  }

  /**
   * Dummy log iterator.
   */
  public class DummyLogIterator implements Log.LogIterator {
    private final ListIterator<Transaction> iter = memLog.listIterator();

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public Transaction next() {
      return iter.next();
    }

    @Override
    public void close() {
      // Do nothing.
    }
  }
}

