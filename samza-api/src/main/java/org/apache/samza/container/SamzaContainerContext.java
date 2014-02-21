package org.apache.samza.container;

import java.util.Collection;

import org.apache.samza.Partition;
import org.apache.samza.config.Config;

public class SamzaContainerContext {
  public final String name;
  public final Config config;
  public final Collection<Partition> partitions;

  /**
   * An immutable context object that can passed to tasks to give them information
   * about the container in which they are executing.
   * @param name The name of the container (either a YARN AM or SamzaContainer).
   * @param config The job configuration.
   * @param partitions The set of input partitions assigned to this container.
   */
  public SamzaContainerContext(
      String name,
      Config config,
      Collection<Partition> partitions) {
    this.name = name;
    this.config = config;
    this.partitions = partitions;
  }
}
