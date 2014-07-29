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

import java.util.List;

/**
 * An interface used to provide useful information for leader election.
 * For now, we only use the size of the ensemble.
 */
public interface ServerState {
  /**
   * Gets the size of the ensemble.
   *
   * @return the size of the ensemble
   */
  int getEnsembleSize() throws Exception;

  /**
   * Gets a list of server ids.
   *
   * @return a list of server ids
   */
  List<String> getServerList() throws Exception;

  /**
   * Gets the minimal quorum size.
   *
   * @return the minimal quorum size.
   */
  int getQuorumSize() throws Exception;

  /**
   * Gets the last accepted proposed epoch.
   *
   * @return the last accepted proposed epoch number
   */
  int getProposedEpoch();
}
