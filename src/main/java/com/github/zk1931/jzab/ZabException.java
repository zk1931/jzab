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
 * Exceptions thrown by {@link Zab}.
 */
public abstract class ZabException extends Exception {

  ZabException() {}

  ZabException(String desc) { super(desc); }

  /**
   * {@link Zab} is in an invalid phase to perform a certain operation.
   */
  public static class InvalidPhase extends ZabException {
    InvalidPhase() {}
    InvalidPhase(String desc) { super(desc); }
  }

  /**
   * Thrown when there are too many pending requests.
   */
  public static class TooManyPendingRequests extends ZabException {
    TooManyPendingRequests() {}
    TooManyPendingRequests(String desc) { super(desc); }
  }
}
