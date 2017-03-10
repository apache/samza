package org.apache.samza.coordinator;

import org.apache.samza.coordinator.leaderelection.LeaderElector;


public interface CoordinationService {
  // life cycle
  void start();
  void stop();


  // facilities for group coordination
  LeaderElector getLeaderElector(); // leaderElector is unique based on the groupId
  //ProcessorLatch getLatch(String latchId);
  //ProcessorBarrier getBarrier(String barrierId);
}
