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

import java.util.concurrent.ExecutionException;

/**
 * Interface for request processor. Different processors are chained together
 * to process request in order.
 */
public interface RequestProcessor {
  int POLL_INTERVAL_MS = 100;

  /**
   * This function should be asynchronous normally, the request should be
   * processed in separate thread.
   *
   * @param request the request object.
   */
  void processRequest(Request request);

  /**
   * Shutdown the processor.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  void shutdown() throws InterruptedException, ExecutionException;
}
