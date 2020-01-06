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

/**
 * This factory instantiates the appropriate implementation of
 * {@link org.apache.samza.system.azureblob.compression.Compression}
 * based on the {@link org.apache.samza.system.azureblob.compression.CompressionType}.
 */
public class CompressionFactory {
  private final static CompressionFactory COMPRESSION_FACTORY_INSTANCE = new CompressionFactory();
  private CompressionFactory() {}

  public static CompressionFactory getInstance() {
    return COMPRESSION_FACTORY_INSTANCE;
  }

  public Compression getCompression(CompressionType compressionType) {
    switch (compressionType) {
      case NONE: return new NoneCompression();
      case GZIP: return new GzipCompression();
      default: throw new IllegalArgumentException("Unknown compression name: " + compressionType.name());
    }

  }
}
