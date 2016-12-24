package org.apache.samza.coordinator.leaderelection;

public interface LeaderElector {
  boolean tryBecomeLeader();
  void resignLeadership();
  boolean amILeader();
}
