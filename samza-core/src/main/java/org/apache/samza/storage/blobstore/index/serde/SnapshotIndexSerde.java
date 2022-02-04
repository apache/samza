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

package org.apache.samza.storage.blobstore.index.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.samza.SamzaException;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.storage.blobstore.index.DirIndex;
import org.apache.samza.storage.blobstore.index.FileBlob;
import org.apache.samza.storage.blobstore.index.FileIndex;
import org.apache.samza.storage.blobstore.index.FileMetadata;
import org.apache.samza.storage.blobstore.index.SnapshotIndex;
import org.apache.samza.storage.blobstore.index.SnapshotMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SnapshotIndexSerde implements Serde<SnapshotIndex> {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotIndexSerde.class);
  private final static ObjectMapper MAPPER = SamzaObjectMapper.getObjectMapper();
  private TypeReference<SnapshotIndex> typeReference;
  private final ObjectWriter objectWriter;
  private final ObjectReader objectReader;

  public SnapshotIndexSerde() {
    MAPPER.addMixIn(SnapshotIndex.class, JsonSnapshotIndexMixin.class)
        .addMixIn(SnapshotMetadata.class, JsonSnapshotMetadataMixin.class)
        .addMixIn(DirIndex.class, JsonDirIndexMixin.class)
        .addMixIn(FileIndex.class, JsonFileIndexMixin.class)
        .addMixIn(FileMetadata.class, JsonFileMetadataMixin.class)
        .addMixIn(FileBlob.class, JsonFileBlobMixin.class);

    this.typeReference = new TypeReference<SnapshotIndex>() { };
    this.objectWriter = MAPPER.writerFor(typeReference);
    this.objectReader = MAPPER.readerFor(typeReference);
  }

  @Override
  public SnapshotIndex fromBytes(byte[] bytes) {
    try {
      LOG.debug("Modules loaded: {}", MAPPER.getRegisteredModuleIds());
      return objectReader.readValue(bytes);
    } catch (Exception exception) {
      throw new SamzaException(String.format("Exception in deserializing SnapshotIndex bytes %s",
          new String(bytes)), exception);
    }
  }

  @Override
  public byte[] toBytes(SnapshotIndex snapshotIndex) {
    try {
      LOG.debug("Modules loaded: {}", MAPPER.getRegisteredModuleIds());
      return objectWriter.writeValueAsBytes(snapshotIndex);
    } catch (Exception exception) {
      throw new SamzaException(String.format("Exception in serializing SnapshotIndex bytes %s", snapshotIndex), exception);
    }
  }
}
