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

package org.apache.samza.checkpoint;

import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;


public class BlobStoreCheckpointMarker implements StateCheckpointMarker {
  public static final String SEPARATOR = ";";
  // backwards compatibility for this unstable api
  private static final short PROTOCOL_VERSION = 1;

  // blob store location id obtained after upload
  private final String blobId;
  // timestamp of when the upload was completed
  private final long createdMillis;

  public BlobStoreCheckpointMarker(String blobId, long createdMillis) {
    this.blobId = blobId;
    this.createdMillis = createdMillis;
  }

  public String getBlobId() {
    return blobId;
  }

  public long getCreatedMillis() {
    return createdMillis;
  }

  public short getProtocolVersion() {
    return PROTOCOL_VERSION;
  }

  public static BlobStoreCheckpointMarker fromString(String message) {
    if (StringUtils.isBlank(message)) {
      throw new IllegalArgumentException("Invalid remote store checkpoint message: " + message);
    }
    String[] parts = message.split(SEPARATOR);
    if (parts.length != 3) {
      throw new IllegalArgumentException("Invalid RemoteStore Metadata offset: " + message);
    }
    if (Short.parseShort(parts[2]) != PROTOCOL_VERSION) {
      throw new IllegalArgumentException("Using different protocol versions fore BlobStoreCheckpointMarker, expected "
          + PROTOCOL_VERSION + ", got " +  parts[2]);
    }
    return new BlobStoreCheckpointMarker(parts[0], Long.parseLong(parts[1]));
  }

  @Override
  public String toString() {
    return String.format("%s%s%s%s%s", blobId, SEPARATOR, createdMillis, SEPARATOR, PROTOCOL_VERSION);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BlobStoreCheckpointMarker that = (BlobStoreCheckpointMarker) o;
    return Objects.equals(blobId, that.blobId) &&
        Objects.equals(createdMillis, that.createdMillis) &&
        Objects.equals(PROTOCOL_VERSION, that.getProtocolVersion());
  }

  @Override
  public int hashCode() {
    return Objects.hash(blobId, createdMillis, PROTOCOL_VERSION);
  }

  @Override
  public String getFactoryName() {
    throw new SamzaException("getFactoryName is not supported for BlobStoreCheckpointMarker");
  }
}
