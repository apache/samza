/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.testUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class FileUtil {
  public static final String TMP_DIR = System.getProperty("java.io.tmpdir");
  private static final Logger LOGGER = LoggerFactory.getLogger(FileUtil.class);

  private FileUtil() {}

  /**
   * Creates a file, along with any parents (if any), in the system-specific Java temporary directory
   * If the file already exists, this method simply returns
   *
   * @param path Path relative to the temporary directory
   * @return True, if the file was created successfully, along with parent files (if any)
   * @throws IOException
   */
  static File createFileInTempDir(String path) throws IOException {
    if (path == null || path.isEmpty()) {
      throw new RuntimeException("Unable to create file - Null or empty path!");
    }
    File file = new File(TMP_DIR, path);
    if (!file.exists()) {
      if (!file.mkdirs()) {
        throw new IOException("Failed to create file");
      }
    }
    return file;
  }

  /**
   * Deletes a given {@link File}, if it exists. If it doesn't exist, it throws a {@link FileNotFoundException}
   * If the given {@link File} is a directory, it recursively deletes the files in the directory, before deleting the
   * directory itself.
   *
   * @param path Reference to the {@link File} to be deleted
   * @return True, if it successfully deleted the given {@link File}. False, otherwise.
   * @throws FileNotFoundException When the given {@link File} does not exist
   * @throws NullPointerException When the given {@link File} reference is null
   */
  static boolean deleteDir(File path) throws FileNotFoundException, NullPointerException {
    if (path == null) {
      throw new NullPointerException("Path cannot be null!");
    }
    if (!path.exists()) {
      throw new FileNotFoundException("File not found: " + path);
    }
    boolean result = true;

    if (path.isDirectory()) {
      for (File f: path.listFiles()) {
        result = result & deleteDir(f);
      }
    }
    return result && path.delete();
  }
}
