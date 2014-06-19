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

package org.apache.zab.transport;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import org.apache.zab.TestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Dummy receiver, used for test purpose only.
 */
class DummyReceiver implements Transport.Receiver {
  ArrayList<DummyTransport.Message> recvMessages
    = new ArrayList<DummyTransport.Message>();
  CountDownLatch expectedMessages;
  boolean disconnected = false;

  public DummyReceiver(CountDownLatch expectedMessages) {
    this.expectedMessages = expectedMessages;
  }

  @Override
  public void onReceived(String source, ByteBuffer message) {
    recvMessages.add(new DummyTransport.Message(source, message));
    if (this.expectedMessages != null) {
      this.expectedMessages.countDown();
    }
  }

  @Override
  public void onDisconnected(String serverId) {
    this.disconnected = true;
    if (this.expectedMessages != null) {
      this.expectedMessages.countDown();
    }
  }
}

/**
 * Test Dummy Transport implementation.
 */
public class DummyTransportTest extends TestBase {
  /**
   * Test the sent messages have been received or not.
   *
   * @throws InterruptedException
   */
  @Test(timeout=1000)
  public void testSendMessage() throws InterruptedException {
    CountDownLatch expectedMessages = new CountDownLatch(3);
    DummyReceiver dr1 = new DummyReceiver(expectedMessages);
    DummyTransport dt1 = new DummyTransport("server1", dr1);
    DummyReceiver dr2 = new DummyReceiver(expectedMessages);
    DummyTransport dt2 = new DummyTransport("server2", dr2);
    DummyReceiver dr3 = new DummyReceiver(expectedMessages);
    DummyTransport dt3 = new DummyTransport("server3", dr3);
    dt1.send("server2", ByteBuffer.wrap("message 1->2".getBytes()));
    dt2.send("server1", ByteBuffer.wrap("message 2->1".getBytes()));
    dt2.send("server3", ByteBuffer.wrap("message 2->3".getBytes()));
    // Wait for all the messages to be delivered.
    expectedMessages.await();
    Assert.assertEquals(dr1.recvMessages.size(), 1);
    Assert.assertEquals(dr2.recvMessages.size(), 1);
    Assert.assertEquals(dr3.recvMessages.size(), 1);
    Assert.assertEquals(dr1.recvMessages.get(0).getServer(), "server2");
    Assert.assertEquals(dr2.recvMessages.get(0).getServer(), "server1");
    Assert.assertEquals(dr3.recvMessages.get(0).getServer(), "server2");
  }

  /**
   * Test the if the delay messages actually perform delays.
   *
   * @throws InterruptedException
   */
  @Test
  public void testTimeout() throws InterruptedException {
    DummyReceiver dr4 = new DummyReceiver(null);
    DummyTransport dt4 = new DummyTransport("server4", dr4);
    DummyReceiver dr5 = new DummyReceiver(null);
    DummyTransport dt5 = new DummyTransport("server5", dr5);
    //dt4.send("server5", null, 200);
    dt4.addDelayMs("server5", 200);
    dt4.send("server5", ByteBuffer.wrap("message 4->5".getBytes()));
    Thread.sleep(100);
    // Since the message is delayed, now we should have not received
    // this message.
    Assert.assertEquals(dr5.recvMessages.size(), 0);
  }

  /**
   * Test if the disconnect message is actually performed.
   *
   * @throws InterruptedException
   */
  @Test(timeout=1000)
  public void testDisconnection() throws InterruptedException {
    CountDownLatch expectedMessages = new CountDownLatch(1);
    DummyReceiver dr6 = new DummyReceiver(expectedMessages);
    DummyTransport dt6 = new DummyTransport("server6", dr6);
    DummyReceiver dr7 = new DummyReceiver(expectedMessages);
    DummyTransport dt7 = new DummyTransport("server7", dr7);
    dt6.disconnect("server7");
    // Wait for the disconnect message received.
    expectedMessages.await();
    Assert.assertTrue(dr7.disconnected);
    Assert.assertFalse(dr6.disconnected);
  }
}
