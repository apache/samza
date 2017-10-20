package org.apache.samza.system.kafka;

import com.google.common.base.Preconditions;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory;

public class KafkaCheckpointLogKey1 {

  public static final String CHECKPOINT_TYPE = "checkpoint";

  private final String grouperFactoryClassName;
  private final TaskName taskName;
  private final String type;

  public KafkaCheckpointLogKey1(String grouperFactoryClassName, TaskName taskName, String type) {
    Preconditions.checkNotNull(grouperFactoryClassName);
    Preconditions.checkNotNull(taskName);
    Preconditions.checkNotNull(type);
    Preconditions.checkState(type.equals(CHECKPOINT_TYPE), String.format("Invalid type provided for checkpoint key. " +
        "Expected: (%s) Actual: (%s)", CHECKPOINT_TYPE, type));

    this.grouperFactoryClassName = grouperFactoryClassName;
    this.taskName = taskName;
    this.type = type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    KafkaCheckpointLogKey1 that = (KafkaCheckpointLogKey1) o;

    if (!grouperFactoryClassName.equals(that.grouperFactoryClassName)) return false;
    if (!taskName.equals(that.taskName)) return false;
    return type.equals(that.type);
  }

  @Override
  public int hashCode() {
    int result = grouperFactoryClassName.hashCode();
    result = 31 * result + taskName.hashCode();
    result = 31 * result + type.hashCode();
    return result;
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

  public static void main(String[] args) throws Exception {
  }
}