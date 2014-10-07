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
   * they arrived. Application MUST apply the transaction in the callback,
   * after deliver returns, Zab assumes the transaction has been applied to
   * state machine.
   *
   * @param zxid zxid of the message
   * @param stateUpdate the incremental state update
   * @param clientId the id of the client who sends the request. The request
   * delivered in RECOVERING phase has clientId sets to null.
   */
  void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId);

  /**
   * Upcall to deliver the flush request. This method is called from the same
   * thread as the deliver callback.
   *
   * @param  flushRequest the flush request.
   */
  void flushed(ByteBuffer flushRequest);

  /**
   * Upcall to serialize the application state using an OutputStream. Upon a
   * call to save, the application writes its state to os.
   *
   * @param os the output stream
   */
  void save(OutputStream os);

  /**
   * Deserializes the state of the application from the InputStream. Once this
   * callback is called. The app restores the state using the input stream.
   *
   * @param is the input stream
   */
  void restore(InputStream is);

  /**
   * Upcall to notify the server it's in recovering phase. Servers in recovering
   * phase shouldn't issue or process any requests.
   */
  void recovering();

  /**
   * Upcall to notify the application who is running on the leader role of ZAB
   * instance the membership changes of Zab cluster. The membership changes
   * include the detection of recovered members or disconnected members in
   * current configuration or new configuration after some one joined or be
   * removed from current configuration. This callback will be called from the
   * same thread as deliver callback.
   *
   * @param activeFollowers current alive followers.
   * @param clusterMembers the members of new configuration.
   */
  void leading(Set<String> activeFollowers, Set<String> clusterMembers);

  /**
   * Upcall to notify the application who is running on the follower role of ZAB
   * instance the membership changes of Zab cluster. The membership changes
   * include the detection of the leader or the new cluster configuration after
   * some servers are joined or removed. This callback will be called from the
   * same thread as deliver callback.
   *
   * @param leader current leader.
   * @param clusterMembers the members of new configuration.
   */
  void following(String leader, Set<String> clusterMembers);
}
