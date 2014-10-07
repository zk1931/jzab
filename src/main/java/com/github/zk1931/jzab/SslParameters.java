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

import java.io.File;

/**
 * Parameters for Ssl.
 */
public class SslParameters {
  private final File keyStore;
  private final String keyStorePassword;
  private final File trustStore;
  private final String trustStorePassword;

  /**
   * @param keyStore keystore file that contains the private key and
   *                 corresponding certificate chain.
   * @param keyStorePassword password for the keystore, or null if the password
   *                         is not set.
   * @param trustStore truststore file that contains trusted CA certificates.
   * @param trustStorePassword password for the truststore, or null if the
   *                           password is not set.
   */
  public SslParameters(File keyStore, String keyStorePassword,
                       File trustStore, String trustStorePassword) {
    this.keyStore = keyStore;
    this.keyStorePassword = keyStorePassword;
    this.trustStore = trustStore;
    this.trustStorePassword = trustStorePassword;
  }

  public SslParameters() {
    this.keyStore = null;
    this.keyStorePassword = null;
    this.trustStore = null;
    this.trustStorePassword = null;
  }

  public File getKeyStore() {
    return this.keyStore;
  }

  public File getTrustStore() {
    return this.trustStore;
  }

  public String getKeyStorePassword() {
    return this.keyStorePassword;
  }

  public String getTrustStorePassword() {
    return this.trustStorePassword;
  }
}
