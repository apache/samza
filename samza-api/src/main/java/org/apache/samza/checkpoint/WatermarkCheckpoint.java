package org.apache.samza.checkpoint;

import java.util.Map;
import org.apache.samza.system.SystemStreamPartition;


public class WatermarkCheckpoint {
  int taskCount;
  Map<String, Long> tasksToTimeStamp;
}
