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

import com.github.zk1931.jzab.proto.ZabMessage.Message;

/**
 * Class that holds both message and source/destionation.
 */
class MessageTuple {
  private final String serverId;
  private final Message message;
  private Zxid zxid;

  public static final MessageTuple REQUEST_OF_DEATH =
      new MessageTuple(null, null);

  public MessageTuple(String serverId, Message message) {
    this(serverId, message, null);
  }

  public MessageTuple(String serverId, Message message, Zxid zxid) {
    this.serverId = serverId;
    this.message = message;
    this.zxid = zxid;
  }

  public String getServerId() {
    return this.serverId;
  }

  public Message getMessage() {
    return this.message;
  }

  public void setZxid(Zxid z) {
    this.zxid = z;
  }

  public Zxid getZxid() {
    return this.zxid;
  }
}
