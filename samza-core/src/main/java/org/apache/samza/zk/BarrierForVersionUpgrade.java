package org.apache.samza.zk;

import java.util.List;


public interface BarrierForVersionUpgrade {
  void leaderStartBarrier(String version,  List<String> processorsNames);
  void waitForBarrier(String version, String processorsName, Runnable callback);
}
