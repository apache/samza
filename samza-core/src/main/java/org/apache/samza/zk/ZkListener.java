package org.apache.samza.zk;

import java.util.List;

public interface ZkListener {
  void onBecomeLeader();
  void onProcessorChange(List<String> processorIds);

  void onNewJobModelAvailable(String version); // start job model update (stop current work)
  void onNewJobModelConfirmed(String version); // start new work according to the new model
}
