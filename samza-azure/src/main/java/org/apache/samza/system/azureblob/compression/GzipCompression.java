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

package org.apache.samza.system.azureblob.compression;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.lang3.Validate;
import org.apache.samza.SamzaException;


/**
 * This class implements the {@link org.apache.samza.system.azureblob.compression.Compression}.
 * It uses GZIPOutputStream to compress the given byte[].
 * The file extension for to be used for this compressed data is ".gz"
 */
public class GzipCompression implements Compression {
  /**
   * {@inheritDoc}
   * @throws SamzaException if compression fails
   */
  @Override
  public byte[] compress(byte[] input) {
    Validate.notNull(input, "Input for compression is null");

    ByteArrayOutputStream byteArrayOutputStream = null;
    GZIPOutputStream gzipOutputStream = null;
    try {
      byteArrayOutputStream = new ByteArrayOutputStream(input.length);
      gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
      gzipOutputStream.write(input);
      gzipOutputStream.close();
      return byteArrayOutputStream.toByteArray();

    } catch (IOException e) {
      throw new SamzaException("Failed to compress.", e);
    } finally {
      try {
        if (gzipOutputStream != null) {
          gzipOutputStream.close();
        }
        if (byteArrayOutputStream != null) {
          byteArrayOutputStream.close();
        }
      } catch (Exception e) {
        throw new SamzaException("Failed to close output streams during compression.", e);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getFileExtension() {
    return ".gz";
  }
}
