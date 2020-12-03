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

package org.apache.samza.storage;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;

import java.util.List;
import java.util.Optional;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.context.Context;
import org.apache.samza.system.ChangelogSSPIterator;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;

/**
 * A mock StorageEngine that stores what it receives from the StorageEngine.
 * Those variables/values can be retrieved directly by variable names.
 */
public class MockStorageEngine implements StorageEngine {

  public static String storeName;
  public static File storeDir;
  public static SystemStreamPartition ssp;

  // Thread-safe list is required because the list is shared across StorageEngine instances
  public static List<IncomingMessageEnvelope> incomingMessageEnvelopes = Collections.synchronizedList(new ArrayList<>());
  public static StoreProperties storeProperties;

  public MockStorageEngine(String storeName, File storeDir, SystemStreamPartition changeLogSystemStreamPartition, StoreProperties properties) {
    MockStorageEngine.storeName = storeName;
    MockStorageEngine.storeDir = storeDir;
    MockStorageEngine.ssp = changeLogSystemStreamPartition;
    MockStorageEngine.storeProperties = properties;
  }

  @Override
  public void init(Context context) {}

  @Override
  public void restore(ChangelogSSPIterator messagesToRestore) {
    while (messagesToRestore.hasNext()) {
      incomingMessageEnvelopes.add(messagesToRestore.next());
    }
  }

  @Override
  public void flush() {
  }

  @Override
  public Optional<Path> checkpoint(CheckpointId id) {
    return Optional.empty();
  }

  @Override
  public void stop() {
  }

  @Override
  public StoreProperties getStoreProperties() {
    return storeProperties;
  }
}
