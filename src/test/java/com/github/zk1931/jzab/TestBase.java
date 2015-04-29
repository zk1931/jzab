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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.junit.Assert.assertTrue;

/**
 * A base class for all the test cases.
 */
public class TestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestBase.class);

  // Used to generate unique port number for unit tests.
  private static AtomicInteger portGenerator = new AtomicInteger(60000);

  @Rule public TestName testName = new TestName();

  /**
   * Logs the beginning and the end of the each test method.
   */
  @Rule
  public TestWatcher watchman= new TestWatcher() {
    @Override
    protected void starting(Description description) {
      int numThreads = Thread.getAllStackTraces().keySet().size();
      LOG.info("STARTING: {}, # of threads: {}", description, numThreads);
      MDC.put("test", description.toString());
    }

    @Override
    protected void failed(Throwable e, Description description) {
      int numThreads = Thread.getAllStackTraces().keySet().size();
      LOG.error("FAILED: {}, # of threads: {}", description, numThreads, e);
      MDC.put("test", "");
    }

    @Override
    protected void succeeded(Description description) {
      int numThreads = Thread.getAllStackTraces().keySet().size();
      LOG.info("SUCCEEDED: {}, # of threads: {}", description, numThreads);
      MDC.put("test", "");
    }
  };

  @Before
  public void createDirectory() {
    Stack<File> stack = new Stack<>();
    File directory = getDirectory();
    if (directory.exists()) {
      stack.push(directory);
    }
    while (!stack.empty()) {
      File dir = stack.pop();
      File[] files = dir.listFiles();
      if (files == null) {
        continue;
      }
      for (File file : files) {
        if (file.isDirectory()) {
          stack.push(file);
        } else {
          assertTrue("Unable to delete file " + file, file.delete());
        }
      }
    }
  }

  /**
   * Creates a data directory for the calling test method.
   *
   * The format of the directory name is "target/data/$classname/$methodname".
   *
   * @return the name of the directory
   */
  protected File getDirectory() {
    String dirName = "target" + File.separator + "data" + File.separator +
                     this.getClass().getCanonicalName() + File.separator +
                     testName.getMethodName();
    File dir = new File(dirName);
    LOG.debug("Creating a data directory: {}", dirName);
    dir.mkdirs();
    return dir;
  }

  /**
   * Returns a unique integer to be used as a port number.
   */
  protected int getUniquePort() {
    return portGenerator.getAndIncrement();
  }

  /**
   * Returns host:port with a given port.
   */
  protected String getHostPort(int port) {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName().toString() +
             ":" + port;
    } catch (UnknownHostException ex) {
      return "localhost" + ":" + port;
    }
  }

  /**
   * Returns host:port string with a unique port.
   */
  protected String getUniqueHostPort() {
    return getHostPort(getUniquePort());
  }

  PersistentState makeInitialState(String serverId, int numTxnsInLog)
      throws IOException {
    PersistentState state =
      new PersistentState(new File(getDirectory(), serverId));
    Log log = state.getLog();
    for (int i = 0; i < numTxnsInLog; ++i) {
      String body = "Txn " + i;
      log.append(new Transaction(new Zxid(0, i),
                                 ByteBuffer.wrap(body.getBytes())));
    }
    return state;
  }

  void appendTxns(Log log, Zxid startZxid, int count) throws IOException {
    for (int i = 0; i < count; ++i) {
      Zxid zxid = new Zxid(startZxid.getEpoch(), startZxid.getXid() + i);
      log.append(new Transaction(zxid, ByteBuffer.wrap("Txn".getBytes())));
    }
  }
}
