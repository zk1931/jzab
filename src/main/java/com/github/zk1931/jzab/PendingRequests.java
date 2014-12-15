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

import java.util.List;
import java.util.LinkedList;

/**
 * Stores different kinds of pending requests.
 */
public class PendingRequests {
  PendingRequests() {}

  /**
   * The pending send requests.
   *
   * The first element of the tuple is the request sent, the second element is
   * the corresponding context
   */
  public final List<Tuple> pendingSends= new LinkedList<>();

  /**
   * The pending flush requests.
   * The first element of the tuple is flush request, the second element is ctx.
   */
  public final List<Tuple> pendingFlushes = new LinkedList<>();

  /**
   * The pending remove requests.
   * The first element of tuple is serverId, the second element is ctx.
   */
  public final List<Tuple> pendingRemoves = new LinkedList<>();

  /**
   * The pending snapshot requests.
   */
  public final List<Object> pendingSnapshots = new LinkedList<>();

  /**
   * The tuple holds both request and ctx.
   */
  public static class Tuple {
    public final Object param;
    public final Object ctx;

    Tuple(Object param, Object ctx) {
      this.param = param;
      this.ctx = ctx;
    }
  }
}
