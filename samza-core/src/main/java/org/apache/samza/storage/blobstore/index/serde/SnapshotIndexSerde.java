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
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.serializers.JsonCheckpointIdMixin;
import org.apache.samza.serializers.Serde;
import org.apache.samza.storage.blobstore.index.DirIndex;
import org.apache.samza.storage.blobstore.index.FileBlob;
import org.apache.samza.storage.blobstore.index.FileIndex;
import org.apache.samza.storage.blobstore.index.FileMetadata;
import org.apache.samza.storage.blobstore.index.SnapshotIndex;
import org.apache.samza.storage.blobstore.index.SnapshotMetadata;


public class SnapshotIndexSerde implements Serde<SnapshotIndex> {

  private final static ObjectMapper MAPPER = new ObjectMapper();
  private TypeReference<SnapshotIndex> typeReference;
  private final ObjectWriter objectWriter;

  public SnapshotIndexSerde() {
    MAPPER.registerModule(new Jdk8Module());
    MAPPER.addMixIn(SnapshotIndex.class, JsonSnapshotIndexMixin.class)
        .addMixIn(SnapshotMetadata.class, JsonSnapshotMetadataMixin.class)
        .addMixIn(DirIndex.class, JsonDirIndexMixin.class)
        .addMixIn(FileIndex.class, JsonFileIndexMixin.class)
        .addMixIn(FileMetadata.class, JsonFileMetadataMixin.class)
        .addMixIn(FileBlob.class, JsonFileBlobMixin.class)
        .addMixIn(CheckpointId.class, JsonCheckpointIdMixin.class);

    this.typeReference = new TypeReference<SnapshotIndex>() { };
    this.objectWriter = MAPPER.writerFor(typeReference);
  }

  @Override
  public SnapshotIndex fromBytes(byte[] bytes) {
    try {
      return MAPPER.readerFor(typeReference).readValue(bytes);
    } catch (Exception exception) {
      throw new SamzaException(String.format("Exception in deserializing SnapshotIndex bytes %s",
          new String(bytes)), exception);
    }
  }

  @Override
  public byte[] toBytes(SnapshotIndex snapshotIndex) {
    try {
      return objectWriter.writeValueAsBytes(snapshotIndex);
    } catch (Exception exception) {
      throw new SamzaException(String.format("Exception in serializing SnapshotIndex bytes %s", snapshotIndex), exception);
    }
  }
}
