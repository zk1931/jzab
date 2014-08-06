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

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * The state machine interface. It contains a list of callbacks which will be
 * called by Zab. It should be implemented by user.
 */
public interface StateMachine {
  /**
   * This method is called only on the leader after the Zxid has been assigned
   * but before proposing the message to followers.
   *
   * @param zxid zxid of the message.
   * @param message the original message
   * @return an idempotent state update based on the original message. This is
   * what gets proposed to followers.
   */
  ByteBuffer preprocess(Zxid zxid, ByteBuffer message);

  /**
   * Upcall to deliver a state update. This method is called from a single
   * thread to ensure that the state updates are applied in the same order
   * they arrived.
   *
   * @param zxid zxid of the message
   * @param stateUpdate the incremental state update
   * @param clientId the id of the client who sends the request. The request
   * delivered in RECOVERING phase has clientId sets to null.
   */
  void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId);

  /**
   * Upcall to serialize the application state using an OutputStream. Upon a
   * call to getState, the application writes its state to os. getState must
   * be called from a different thread of the one that calls deliver to avoid
   * blocking the delivery of the message.
   *
   * @param os the output stream
   */
  void getState(OutputStream os);

  /**
   * Deserializes the state of the application from the InputStream. Once this
   * callback is called. The app restores the state using the input stream. This
   * method must be called from the same thread of the one calls deliver to
   * avoid ending up in inconsistent state.
   *
   * @param is the input stream
   */
  void setState(InputStream is);

  /**
   * Upcall to notify all the servers the cluster configuration changes. This
   * happens when the server receives COP message.
   *
   * @param servers the servers in new configuration.
   */
  void clusterChange(Set<String> servers);

  /**
   * Upcall to notify the server it's in recovering phase. Servers in recovering
   * phase shouldn't issue or process any requests.
   */
  void recovering();

  /**
   * Upcall to notify the application who is running on the leader role of ZAB
   * instance. This callback will be called once ZAB enters broadcasting phase
   * or the membership of active followers is changed.
   *
   * @param activeFollowers current alive followers.
   */
  void leading(Set<String> activeFollowers);

  /**
   * Upcall to notify the application who is running on follower role of ZAB
   * instance. This callback will be called once the ZAB enters the broadcasting
   * phase.
   *
   * @param leader the elected leader for this follower.
   */
  void following(String leader);
}
