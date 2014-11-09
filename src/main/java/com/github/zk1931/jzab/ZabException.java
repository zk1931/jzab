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

/**
 * Exceptions for Jzab.
 */
public abstract class ZabException extends Exception {

  ZabException() {}

  ZabException(String desc) { super(desc); }

  /**
   * Sends request in non-broadcasting phase.
   */
  public static class NotBroadcastingPhaseException extends ZabException {
    public NotBroadcastingPhaseException() {}
    public NotBroadcastingPhaseException(String desc) { super(desc); }
  }

  /**
   * Exception of to many pending requests in Jzab.
   */
  public static class ToManyPendingRequests extends ZabException {
    public ToManyPendingRequests() {}
    public ToManyPendingRequests(String desc) { super(desc); }
  }
}
