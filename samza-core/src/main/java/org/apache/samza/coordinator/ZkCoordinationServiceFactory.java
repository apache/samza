package org.apache.samza.coordinator;

import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.config.Config;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.zk.ZkKeyBuilder;
import org.apache.samza.zk.ZkUtils;


//TODO move to zk package
public class ZkCoordinationServiceFactory implements CoordinationServiceFactory{
  private final ZkConfig zkConfig;
  private final ZkUtils zkUtils;
  private final String processorId;

  public ZkCoordinationServiceFactory(String processorId, Config config, String groupId) {
    zkConfig = new ZkConfig(config);
    ZkClient zkClient = new ZkClient(zkConfig.getZkConnect(), zkConfig.getZkSessionTimeoutMs(), zkConfig.getZkConnectionTimeoutMs());
    zkUtils = new ZkUtils(groupId, new ZkKeyBuilder(groupId), zkClient, zkConfig.getZkConnectionTimeoutMs());
    this.processorId = processorId;
  }

  @Override
  public CoordinationService getCoordinationService(String groupId) {
    return new ZkCoordinationService(processorId, zkConfig, zkUtils);
  }
}
