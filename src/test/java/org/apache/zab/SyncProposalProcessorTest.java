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

// Need to import logback here to set the root log level to INFO.
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import org.apache.zab.proto.ZabMessage.Message;
import org.apache.zab.transport.NettyTransport;
import org.apache.zab.transport.Transport;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

/**
 * Tests the performance of SyncProposalProcessor with different implementations
 * of the Log interfaces.
 */
@RunWith(Parameterized.class)
public class SyncProposalProcessorTest extends TestBase {
  // Don't log DEBUG since this test is for performance benchmark.
  private static final Logger LOG =
    (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
  static {
    LOG.setLevel(Level.INFO);
  }
  public Log log;

  public SyncProposalProcessorTest(Log log) {
    this.log = log;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> instancesToTest() throws Exception {
    // hardcode the directory for now.
    String dirName = "target" + File.separator + "data" + File.separator +
                     "LogTest";
    File dir = new File(dirName);
    dir.mkdirs();
    return Arrays.asList(
      new Object[][] {
        {new SimpleLog(new File(dir, "txn.log"))}
      });
  }

  public void benchmark(int epoch, int transactionSize, int numTransactions,
                        int batchSize)
      throws Exception {
    LOG.info("Starting the test for {}", log.getClass().getName());
    LOG.info("epoch       = {}", epoch);
    LOG.info("txn size    = {}", transactionSize);
    LOG.info("num_ops     = {}", numTransactions);
    LOG.info("batch_size  = {}", batchSize);
    final String leader = getUniqueHostPort();
    int count = numTransactions;
    final Zxid expectedZxid = new Zxid(epoch, count - 1);
    final CountDownLatch latch = new CountDownLatch(1);
    final int ackCount = 0;

    class TestReceiver implements Transport.Receiver {
      int ackCount = 0;
      @Override
      public void onReceived(String source, ByteBuffer message) {
        try {
          byte[] buffer = new byte[message.remaining()];
          message.get(buffer);
          Message msg = Message.parseFrom(buffer);
          Zxid zxid = MessageBuilder.fromProtoZxid(msg.getAck().getZxid());
          ackCount++;
          if (zxid.equals(expectedZxid)) {
            latch.countDown();
          } else {
            LOG.debug("received {} {}", zxid, expectedZxid);
          }
        } catch (Exception ex) {
          Assert.fail("Unexpected exception: " + ex.getMessage());
        }
      }
      @Override
      public void onDisconnected(String source) {
      }
    }

    TestReceiver receiver = new TestReceiver();
    Transport transport = new NettyTransport(leader, receiver);
    SyncProposalProcessor processor = new SyncProposalProcessor(log, transport,
                                                                batchSize);
    long startTime = System.nanoTime();
    String message = new String(new char[transactionSize]).replace('\0', 'a');
    for (int i = 0; i < count; i++) {
      Transaction txn = new Transaction(new Zxid(epoch, i),
                            ByteBuffer.wrap(message.getBytes()));
      Message proposal = MessageBuilder.buildProposal(txn);
      MessageTuple request = new MessageTuple(leader, proposal);
      processor.processRequest(request);
    }
    latch.await();
    double endTimeSec = (double) (System.nanoTime() - startTime) /
                        (1000 * 1000 * 1000);
    LOG.info("runtime     = {} seconds", endTimeSec);
    LOG.info("throughput  = {} ops/sec", (int)(numTransactions / endTimeSec));
    LOG.info("throughput  = {} MB/sec",
             numTransactions * transactionSize / endTimeSec / 1024 / 1024);
    LOG.info("ack count   = {}", receiver.ackCount);
    processor.shutdown();
    transport.shutdown();
  }

  @Ignore @Test
  public void testPerformance() throws Exception {
    benchmark(0, 1024, 1000, 1);
    benchmark(1, 128, 100000, 1000);
    benchmark(2, 1024, 100000, 1000);
  }
}
