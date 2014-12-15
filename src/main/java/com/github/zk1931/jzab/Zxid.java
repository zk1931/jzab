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
  private final long epoch;
  private final long xid;
  private static final int ZXID_LENGTH = 16;
  static final Zxid ZXID_NOT_EXIST = new Zxid(0, -1);

  public Zxid(long epoch, long xid) {
    this.epoch = epoch;
    this.xid = xid;
  }

  public static int getZxidLength() {
    return ZXID_LENGTH;
  }

  public long getEpoch() {
    return this.epoch;
  }

  public long getXid() {
    return this.xid;
  }

  public static Zxid fromByteArray(byte[] bytes) throws IOException {
    DataInputStream in = new DataInputStream(
                         new BufferedInputStream(
                         new ByteArrayInputStream(bytes)));
    long epoch = in.readLong();
    long xid = in.readLong();
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
    out.writeLong(this.epoch);
    out.writeLong(this.xid);
    out.flush();
    return bout.toByteArray();
  }

  @Override
  public int compareTo(Zxid zxid) {
    long res = (this.epoch != zxid.epoch)? this.epoch - zxid.epoch :
                                           this.xid - zxid.xid;
    return (int)res;
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
    return 0;
  }

  public String toSimpleString() {
    return String.format("%015d_%015d", this.epoch, this.xid);
  }

  public static Zxid fromSimpleString(String zxid) {
    String []str = zxid.split("_");
    if (str.length != 2) {
      throw new RuntimeException("Can't convert string to zxid, wrong format.");
    }
    return new Zxid(Long.parseLong(str[0]), Long.parseLong(str[1]));
  }
}
