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
package org.apache.samza.metadatastore;

import org.apache.samza.container.TaskName;
import org.apache.samza.runtime.LocationId;

import java.util.Collection;
import java.util.Map;

/**
 * Store abstraction responsible for managing the metadata of a samza job and is agnostic of the
 * deployment model(yarn/standalone) of the samza job. Serialization and deserialization of the metadata should be
 * hidden within the implementation.
 */
public interface MetadataStore {

  /**
   * Initializes the metadata store, if applicable, setting up the underlying resources
   * and connections to the store endpoints. Upon successful completion of this method,
   * metadata store is considered available to accept the client operations.
   */
  void init();

  /**
   * Reads and returns the {@link TaskName} to {@link LocationId} association for a samza job
   * from the underlying storage layer. Returns empty map if the task locality does not exist.
   * @return the task locality.
   */
  Map<TaskName, LocationId> readTaskLocality();

  /**
   * Reads and returns the processorId to {@link LocationId} association for a samza job
   * from the underlying storage layer. Returns empty map if the processor locality does not exist.
   * @return the processor locality.
   */
  Map<String, LocationId> readProcessorLocality();

  /**
   * Stores the taskLocality of the samza job to the underlying storage layer.
   * @param processorId
   * @param taskLocality
   * @param jmxUrl
   * @param jmxTunnelingUrl
   */
  void writeTaskLocality(String processorId, Map<TaskName, LocationId> taskLocality, String jmxUrl, String jmxTunnelingUrl);

  /**
   * Deletes the locality of the tasks associated with a samza job.
   * @param taskNames the list of task names for which task locality should be deleted.
   */
  void deleteTaskLocality(Collection<TaskName> taskNames);

  /**
   * Closes the metadata store, if applicable, relinquishing all the underlying resources
   * and connections.
   */
  void close();
}
