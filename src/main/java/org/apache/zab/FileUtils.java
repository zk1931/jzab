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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

/**
 * A static class that contains file-related utility methods.
 */
public final class FileUtils {
  private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

  /**
   * Disables the constructor.
   */
  private FileUtils() {
  }

  /**
   * Atomically writes an integer to a file.
   *
   * This method writes an integer to a file by first writing the integer to a
   * temporary file and then atomically moving it to the destination,
   * overwriting the destination file if it already exists.
   *
   * @throws IOException if an I/O error occurs.
   */
  public static void writeIntToFile(int value, File file) throws IOException {
    // Create a temp file in the same directory as the file parameter.
    File temp = File.createTempFile(file.getName(), null,
                                    file.getAbsoluteFile().getParentFile());
    try (FileOutputStream fos = new FileOutputStream(temp);
         DataOutputStream dos = new DataOutputStream(fos)) {
      dos.writeInt(value);
      fos.getChannel().force(true);
    }
    Files.move(temp.toPath(), file.toPath(), ATOMIC_MOVE);
    LOG.debug("Atomically moved {} to {}", temp, file);
  }

  /**
   * Reads an integer from a file that was created by the
   * {@link #writeIntToFile(int, File) writeIntToFile} method.
   *
   * @return the integer value in the file
   * @throws IOException if an I/O error occurs.
   */
  public static int readIntFromFile(File file) throws IOException {
    try (FileInputStream fis = new FileInputStream(file);
        DataInputStream dis = new DataInputStream(fis)) {
      int value = dis.readInt();
      return value;
    }
  }
}
