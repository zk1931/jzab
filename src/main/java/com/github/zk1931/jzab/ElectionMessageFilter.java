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

import com.github.zk1931.jzab.proto.ZabMessage.Message.MessageType;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

/**
 * ElectionMessageFilter is a filter class which acts as a successor of
 * MessageQueueFilter. It handles and filters ELECTION_INFO message, which will
 * be passed to its Election object.
 */
class ElectionMessageFilter extends MessageQueueFilter {
  private final Election election;

  ElectionMessageFilter(BlockingQueue<MessageTuple> messageQueue,
                        Election election) {
    super(messageQueue);
    this.election = election;
  }

  @Override
  protected MessageTuple getMessage(int timeoutMs)
      throws InterruptedException, TimeoutException {
    int startMs = (int)(System.nanoTime() / 1000000);
    while (true) {
      int nowMs = (int)(System.nanoTime() / 1000000);
      int remainMs = timeoutMs - (nowMs - startMs);
      if (remainMs < 0) {
        remainMs = 0;
      }
      MessageTuple tuple = super.getMessage(remainMs);
      if (tuple.getMessage().getType() == MessageType.ELECTION_INFO) {
        this.election.reply(tuple);
      } else {
        return tuple;
      }
    }
  }
}
