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
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

public class TestGzipCompression {
  private GzipCompression gzipCompression;

  @Before
  public void setup() {
    gzipCompression = new GzipCompression();
  }

  @Test
  public void testCompression() throws IOException {
    byte[] input = "This is fake input data".getBytes();
    byte[] result = compress(input);

    Assert.assertArrayEquals(gzipCompression.compress(input), result);
  }

  @Test
  public void testCompressionEmpty() throws IOException {
    byte[] input = "".getBytes();
    byte[] result = compress(input);

    Assert.assertArrayEquals(gzipCompression.compress(input), result);
  }

  @Test(expected = RuntimeException.class)
  public void testCompressionNull() {
    byte[] input = null;
    gzipCompression.compress(input);
  }

  @Test
  public void testCompressionZero() throws IOException {
    byte[] input = new byte[100];
    byte[] result = compress(input);

    Assert.assertArrayEquals(gzipCompression.compress(input), result);
  }

  private byte[] compress(byte[] input) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(input.length);
    GZIPOutputStream gzipOS = new GZIPOutputStream(bos);
    gzipOS.write(input);
    gzipOS.close();
    gzipOS.close();
    bos.close();
    return bos.toByteArray();
  }
}
