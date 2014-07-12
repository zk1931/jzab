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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Simple implementation of Zxid.
 */
public class Zxid implements Comparable<Zxid> {
  private final int epoch;
  private final int xid;
  private static final int ZXID_LENGTH = 8;
  static final Zxid ZXID_NOT_EXIST = new Zxid(0, -1);

  public Zxid(int epoch, int xid) {
    this.epoch = epoch;
    this.xid = xid;
  }

  public static int getZxidLength() {
    return ZXID_LENGTH;
  }

  public int getEpoch() {
    return this.epoch;
  }

  public int getXid() {
    return this.xid;
  }

  public static Zxid fromByteArray(byte[] bytes) throws IOException {
    DataInputStream in = new DataInputStream(
                         new BufferedInputStream(
                         new ByteArrayInputStream(bytes)));
    int epoch = in.readInt();
    int xid = in.readInt();
    return new Zxid(epoch, xid);
  }

  /**
   * Serialize this Zxid into a fixed size (8 bytes) byte array.

   * The resulting byte array can be deserialized with fromByteArray.
   * @return an array of bytes.
   * @throws IOException in case of an IO failure
   */
  public byte[] toByteArray() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(bout));
    out.writeInt(this.epoch);
    out.writeInt(this.xid);
    out.flush();
    return bout.toByteArray();
  }

  // Check whether z1 and z2 are contiguous and z1 precedes z2.
  public static boolean isContiguous(Zxid z1, Zxid z2) {
    return z1.epoch == z2.epoch && (z2.xid - z1.xid == 1);
  }

  @Override
  public int compareTo(Zxid zxid) {
    return (this.epoch != zxid.epoch)? this.epoch - zxid.epoch :
                                       this.xid - zxid.xid;
  }

  @Override
  public String toString() {
    return String.format("Zxid [epoch: %s, xid: %s]", this.epoch, this.xid);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof Zxid)) {
      return false;
    }
    Zxid z = (Zxid)o;
    return compareTo(z) == 0;
  }

  @Override
  public int hashCode() {
    int hash = 17;
    hash = hash * 31 + this.epoch;
    hash = hash * 31 + this.xid;
    return hash;
  }
}
