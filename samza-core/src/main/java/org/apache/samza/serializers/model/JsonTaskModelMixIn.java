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

import java.util.Set;

import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A mix-in Jackson class to convert Samza's TaskModel to/from JSON.
 */
public abstract class JsonTaskModelMixIn {
  @JsonCreator
  public JsonTaskModelMixIn(@JsonProperty("task-name") TaskName taskName, @JsonProperty("system-stream-partitions") Set<SystemStreamPartition> systemStreamPartitions, @JsonProperty("changelog-partition") Partition changelogPartition) {
  }

  @JsonProperty("task-name")
  abstract TaskName getTaskName();

  @JsonProperty("system-stream-partitions")
  abstract Set<SystemStreamPartition> getSystemStreamPartitions();

  @JsonProperty("changelog-partition")
  abstract Partition getChangelogPartition();
}