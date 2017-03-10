package org.apache.samza.coordinator;

import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.leaderelection.LeaderElector;
import org.apache.samza.zk.ZkLeaderElector;
import org.apache.samza.zk.ZkUtils;


public class ZkCoordinationService implements CoordinationService {
  public final ZkConfig zkConfig;
  public final ZkUtils zkUtils;
  public final String processorIdStr;

  public ZkCoordinationService(String processorId, ZkConfig zkConfig, ZkUtils zkUtils) {
    this.zkConfig = zkConfig;
    this.zkUtils = zkUtils;
    this.processorIdStr = processorId;
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {

  }

  @Override
  public LeaderElector getLeaderElector() {
    return new ZkLeaderElector(processorIdStr, zkUtils);
  }
}
