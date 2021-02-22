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
package org.apache.samza.coordinator.stream.messages;

/**
 * SetJobCoordinatorMetadataMessage is used internally by the Samza framework to
 * persist {@link org.apache.samza.job.JobCoordinatorMetadata} in coordinator stream.
 *
 * Structure of the message looks like:
 * {
 *     Key: meta-key
 *     Type: set-job-coordinator-metadata
 *     Source: "SamzaContainer"
 *     MessageMap:
 *     {
 *         "epoch-id": epoch identifier of the job coordinator,
 *         "job-model-id": identifier associated with the snapshot of job model used by the job coordinator,
 *         "config-id": identifier associated with the snapshot of the configuration used by the job coordinator
 *     }
 * }
 * */
public class SetJobCoordinatorMetadataMessage extends CoordinatorStreamMessage {
  private static final String META_KEY = "meta-key";
  public static final String TYPE = "set-job-coordinator-metadata";

  public SetJobCoordinatorMetadataMessage(CoordinatorStreamMessage message) {
    super(message.getKeyArray(), message.getMessageMap());
  }

  public SetJobCoordinatorMetadataMessage(String source, String clusterType, String metaMessage) {
    super(source);
    setType(TYPE);
    setKey(clusterType);
    putMessageValue(META_KEY, metaMessage);
  }

  public String getJobCoordinatorMetadata() {
    return getMessageValue(META_KEY);
  }
}
