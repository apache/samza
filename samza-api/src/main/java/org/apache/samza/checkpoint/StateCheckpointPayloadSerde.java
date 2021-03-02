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

public interface StateCheckpointPayloadSerde<T extends StateCheckpointMarker> {
  /**
   * Serializes the payload of the StateCheckpointMarker
   * @param payload StateCheckpointMarker to be serialized
   * @return String representation of the StateCheckpointMarker
   */
  String serialize(T payload);

  /**
   * Deserializes the payload of the StateCheckpointMarker that can be used by the respective
   * Restore and Backup manager based on the StateBackendFactory
   * @param data String representation of the StateCheckpointMarker to be deserialized
   * @return StateCheckpointMarker build from the data
   */
  T deserialize(String data);
}
