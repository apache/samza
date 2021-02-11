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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskModel;

/**
 * A mix-in Jackson class to convert {@link org.apache.samza.job.model.ContainerModel} to JSON.
 * Notes:
 * 1) Constructor is not needed because this mixin is not used for deserialization. See {@link SamzaObjectMapper}.
 * 2) It is unnecessary to use {@link com.fasterxml.jackson.annotation.JsonIgnoreProperties#ignoreUnknown()} here since
 * {@link SamzaObjectMapper} already uses custom deserialization code for the
 * {@link org.apache.samza.job.model.ContainerModel}.
 * 3) See {@link SamzaObjectMapper} for more context about why the JSON keys are named in this specified way.
 */
public abstract class JsonContainerModelMixIn {
  /**
   * This is intentionally not "id" for backwards compatibility reasons. See {@link SamzaObjectMapper} for more details.
   */
  static final String PROCESSOR_ID_KEY = "processor-id";
  /**
   * This is used for backwards compatibility. See {@link SamzaObjectMapper} for more details.
   */
  static final String CONTAINER_ID_KEY = "container-id";
  static final String TASKS_KEY = "tasks";

  @JsonProperty(PROCESSOR_ID_KEY)
  abstract String getId();

  @JsonProperty(TASKS_KEY)
  abstract Map<TaskName, TaskModel> getTasks();
}

