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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

/**
 * Abstract class for Zab implementation.
 */
public abstract class Zab {
  protected StateMachine stateMachine;
  protected ZabConfig config;

  /**
   * The state of the Zab.
   */
  public enum ZabState {
    LOOKING,
    LEADING,
    FOLLOWING
  }

  /**
   * Constructs an Zab object.
   *
   * @param stateMachine the state machine implementation
   * @param prop the properties object stores the configuration
   */
  public Zab(StateMachine stateMachine, Properties prop) {
    this.stateMachine = stateMachine;
    this.config = new ZabConfig(prop);
  }

  /**
   * Sends a message to Zab. Any one can call call this
   * interface. Under the hood, followers forward requests
   * to the leader and the leader will be responsible for
   * broadcasting.
   *
   * @param message message to send through Zab
   * @throws IOException in case of IO failures
   */
  public abstract void send(ByteBuffer message) throws IOException;

  /**
   * Trims a prefix of the log. Used to reduce the size
   * of log after snapshot.
   *
   * @param zxid trim the log to zxid(including zxid)
   */
  public abstract void trimLogTo(Zxid zxid);
}
