package org.apache.samza.container.host;

import org.junit.Test;
import static org.junit.Assert.*;


public class TestLinuxCgroupStatistics {
  @Test
  public void testGetCgroupCPUThrottleRatio() {
    LinuxCgroupStatistics linuxCgroupStatistics = new LinuxCgroupStatistics(10.0);
    assertEquals(linuxCgroupStatistics.getCgroupCpuThrottleRatio(), 10.0, 0.05);
  }
}
