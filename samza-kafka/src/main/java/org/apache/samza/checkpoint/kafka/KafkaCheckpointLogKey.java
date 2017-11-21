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
package org.apache.samza.checkpoint.kafka;

import com.google.common.base.Preconditions;
import org.apache.samza.container.TaskName;

/**
 * The key used for messages that are written to the Kafka checkpoint log.
 */
public class KafkaCheckpointLogKey {

  public static final String CHECKPOINT_KEY_TYPE = "checkpoint";
  /**
   * The SystemStreamPartitionGrouperFactory configured for this job run. Since, checkpoints of different
   * groupers are not compatible, we persist and validate them across job runs.
   */
  private final String grouperFactoryClassName;
  /**
   * The taskName corresponding to the checkpoint. Checkpoints in Samza are stored per-task.
   */
  private final TaskName taskName;
  /**
   * The type of this key. Used for supporting multiple key-types. Currently, the only supported key-type is
   * "checkpoint"
   */
  private final String type;

  public KafkaCheckpointLogKey(String type, TaskName taskName, String grouperFactoryClassName) {
    Preconditions.checkNotNull(grouperFactoryClassName);
    Preconditions.checkNotNull(taskName);
    Preconditions.checkNotNull(type);
    Preconditions.checkState(!grouperFactoryClassName.isEmpty(), "Empty grouper factory class provided");

    Preconditions.checkState(type.equals(CHECKPOINT_KEY_TYPE), String.format("Invalid type provided for checkpoint key. " +
        "Expected: (%s) Actual: (%s)", CHECKPOINT_KEY_TYPE, type));

    this.grouperFactoryClassName = grouperFactoryClassName;
    this.taskName = taskName;
    this.type = type;
  }

  public String getGrouperFactoryClassName() {
    return grouperFactoryClassName;
  }

  public TaskName getTaskName() {
    return taskName;
  }

  public String getType() {
    return type;
  }

  /**
   * Two {@link KafkaCheckpointLogKey}s are equal iff their grouperFactory class, taskName and type are equal.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    KafkaCheckpointLogKey that = (KafkaCheckpointLogKey) o;

    if (!grouperFactoryClassName.equals(that.grouperFactoryClassName)) {
      return false;
    }
    if (!taskName.equals(that.taskName)) {
      return false;
    }
    return type.equals(that.type);
  }

  /**
   * Two {@link KafkaCheckpointLogKey}s are equal iff their grouperFactory class, taskName and type are equal.
   */
  @Override
  public int hashCode() {
    int result = grouperFactoryClassName.hashCode();
    result = 31 * result + taskName.hashCode();
    result = 31 * result + type.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return String.format("KafkaCheckpointLogKey[factoryClass: %s, taskName: %s, type: %s]",
        grouperFactoryClassName, taskName, type);
  }
}