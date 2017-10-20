package org.apache.samza.checkpoint.kafka;

import com.google.common.base.Preconditions;
import org.apache.samza.container.TaskName;

public class KafkaCheckpointLogKey {

  public static final String CHECKPOINT_TYPE = "checkpoint";

  private final String grouperFactoryClassName;
  private final TaskName taskName;
  private final String type;

  public KafkaCheckpointLogKey(String grouperFactoryClassName, TaskName taskName, String type) {
    Preconditions.checkNotNull(grouperFactoryClassName);
    Preconditions.checkNotNull(taskName);
    Preconditions.checkNotNull(type);
    Preconditions.checkState(type.equals(CHECKPOINT_TYPE), String.format("Invalid type provided for checkpoint key. " +
        "Expected: (%s) Actual: (%s)", CHECKPOINT_TYPE, type));

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

  public static void main(String[] args) throws Exception {
  }
}