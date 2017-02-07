package org.apache.samza.zk;


public interface ZkController {
  void register ();
  boolean isLeader();
  void notifyJobModelChange(String version);
  void stop();
  void listenToProcessorLiveness();
  String currentJobModelVersion();
}
