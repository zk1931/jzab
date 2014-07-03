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

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.zab.proto.ZabMessage.Message;
import org.apache.zab.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class will send acknowledgment to leader.
 */
public class AckProcessor implements RequestProcessor,
                                     Callable<Integer> {

  private final BlockingQueue<Request> ackQueue =
      new LinkedBlockingQueue<Request>();

  private final Transport transport;

  private static final Logger LOG =
      LoggerFactory.getLogger(AckProcessor.class);

  Future<Integer> ft;

  /**
   * Constructs an AckProcessor.
   *
   * @param transport it will be used to send acknowledgment.
   */
  public AckProcessor(Transport transport) {
    this.transport = transport;
    // Starts running.
    ft = Executors.newSingleThreadExecutor().submit(this);
  }

  @Override
  public void processRequest(Request request) {
    this.ackQueue.add(request);
  }

  @Override
  public Integer call() throws Exception {

    LOG.debug("AckProcessor gets started.");

    while (true) {
      Request req = ackQueue.take();
      String leaderId = req.getServerId();
      Zxid ackZxid = MessageBuilder.fromProtoZxid(req.getMessage()
                                                     .getProposal()
                                                     .getZxid());
      Message ack = MessageBuilder.buildAck(ackZxid);
      ByteBuffer buffer = ByteBuffer.wrap(ack.toByteArray());
      this.transport.send(leaderId, buffer);
    }

  }

}
