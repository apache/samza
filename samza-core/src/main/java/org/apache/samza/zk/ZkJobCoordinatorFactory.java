package org.apache.samza.zk;

import org.apache.samza.config.Config;
import org.apache.samza.config.JavaJobConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorFactory;
import org.apache.samza.processor.SamzaContainerController;

public class ZkJobCoordinatorFactory implements JobCoordinatorFactory {
  /**
   * Method to instantiate an implementation of JobCoordinator
   *
   * @param processorId Indicates the StreamProcessor's id to which this Job Coordinator is associated with
   * @param config      Configs relevant for the JobCoordinator TODO: Separate JC related configs into a "JobCoordinatorConfig"
   * @return An instance of IJobCoordinator
   */
  @Override
  public JobCoordinator getJobCoordinator(int processorId, Config config, SamzaContainerController containerController) {
    JavaJobConfig jobConfig = new JavaJobConfig(config);
    String groupName = String.format("%s-%s", jobConfig.getJobName(), jobConfig.getJobId());
    ZkConfig zkConfig = new ZkConfig(config);
    ScheduleAfterDebounceTime debounceTimer = new ScheduleAfterDebounceTime();
    return new ZkJobCoordinator(
        processorId,
        config,
        debounceTimer,
        new ZkUtils(
            new ZkKeyBuilder(groupName),
            zkConfig.getZkConnect(),
            debounceTimer,
            String.valueOf(processorId),
            zkConfig.getZkSessionTimeoutMs(),
            zkConfig.getZkConnectionTimeoutMs()),
        containerController);
  }
}
