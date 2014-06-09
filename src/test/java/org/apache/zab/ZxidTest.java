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

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

/**
 *  Test Zxid.
 */
public class ZxidTest {
  @Test
  public void cmpTest() {
    Zxid z1 = new Zxid(1, 0);
    Zxid z2 = new Zxid(1, 0);
    Assert.assertTrue(z1.compareTo(z2) == 0);

    z1 = new Zxid(1, 0);
    z2 = new Zxid(1, 1);
    Assert.assertTrue(z1.compareTo(z2) < 0);

    z1 = new Zxid(2, 0);
    z2 = new Zxid(1, 1);
    Assert.assertTrue(z1.compareTo(z2) > 0);
  }

  @Test
  public void contiguousTest() {
    Zxid z1 = new Zxid(10, 1);
    Zxid z2 = new Zxid(10, 2);
    Assert.assertTrue(Zxid.isContiguous(z1, z2));
    Assert.assertFalse(Zxid.isContiguous(z2, z1));

    z2 =  new Zxid(10, 3);
    Assert.assertFalse(Zxid.isContiguous(z1, z2));

    z2 = new Zxid(11, 2);
    Assert.assertFalse(Zxid.isContiguous(z1, z2));
  }

  @Test
  public void serializeTest() throws IOException {
    Zxid z1 = new Zxid(1, 20);
    byte[] bytes = z1.toByteArray();
    Assert.assertEquals(bytes.length, 8);
    Zxid z2 = Zxid.fromByteArray(bytes);
    Assert.assertTrue(z1.compareTo(z2) == 0);
  }

  @Test
  public void toStringTest() {
    Zxid z = new Zxid(10, 3);
    Assert.assertEquals("Zxid [epoch : 10, xid : 3]", z.toString());
  }
}
