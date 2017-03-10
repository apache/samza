package org.apache.samza.coordinator;

public interface LeaderElectorListener {
  void onBecomingLeader();
}
