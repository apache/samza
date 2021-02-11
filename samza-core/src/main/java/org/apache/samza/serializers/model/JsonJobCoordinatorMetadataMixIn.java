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
package org.apache.samza.serializers.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A mix-in Jackson class to convert {@link org.apache.samza.job.JobCoordinatorMetadata} to/from JSON
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class JsonJobCoordinatorMetadataMixIn {

  @JsonCreator
  public JsonJobCoordinatorMetadataMixIn(@JsonProperty("epoch-id") String epochId,
      @JsonProperty("config-id") String configId, @JsonProperty("job-model-id") String jobModelId) {
  }

  @JsonProperty("epoch-id")
  abstract String getEpochId();

  @JsonProperty("config-id")
  abstract String getConfigId();

  @JsonProperty("job-model-id")
  abstract String getJobModelId();
}
