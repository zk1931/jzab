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
import java.io.FileNotFoundException;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test FileUtils.
 */
public class FileUtilsTest extends TestBase {
  @Test
  public void testReadWriteLong() throws IOException {
    File file = new File(getDirectory(), "test");
    for (int i = 0; i < 10; i++) {
      FileUtils.writeLongToFile(i, file);
      Assert.assertEquals(i, FileUtils.readLongFromFile(file));
    }
  }

  @Test(expected=FileNotFoundException.class)
  public void testFileNotFound() throws IOException {
    FileUtils.readLongFromFile(new File(getDirectory(), "missing"));
  }
}
