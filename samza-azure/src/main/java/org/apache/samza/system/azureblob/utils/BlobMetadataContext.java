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

package org.apache.samza.system.azureblob.utils;

/**
 * Properties about a Blob which can then be used to generate metadata for the blob
 */
public class BlobMetadataContext {
  private final String streamName;
  private final long blobSize;
  private final long numberOfMessagesInBlob;

  public BlobMetadataContext(String streamName, long blobSize, long numberOfMessagesInBlob) {
    this.streamName = streamName;
    this.blobSize = blobSize;
    this.numberOfMessagesInBlob = numberOfMessagesInBlob;
  }

  public String getStreamName() {
    return streamName;
  }

  public long getBlobSize() {
    return blobSize;
  }

  public long getNumberOfMessagesInBlob() {
    return numberOfMessagesInBlob;
  }
}
