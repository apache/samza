package org.apache.samza.config;

import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.apache.samza.zk.ZkCoordinationUtilsFactory;
import org.junit.Test;


public class TestJobCoordinatorConfig {

  private final static String ANOTHER_FACTORY = "AnotherFactory";

  @Test
  public void testJobCoordinationUtilsFactoryConfig() {

    Map<String, String> map = new HashMap<>();
    JobCoordinatorConfig jConfig = new JobCoordinatorConfig(new MapConfig(map));

    // test default value
    Assert.assertEquals(ZkCoordinationUtilsFactory.class.getName(), jConfig.getJobCoordinationUtilsFactoryClassName());

    map.put(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, ANOTHER_FACTORY);
    jConfig = new JobCoordinatorConfig(new MapConfig(map));
    Assert.assertEquals(ANOTHER_FACTORY, jConfig.getJobCoordinationUtilsFactoryClassName());
  }
}
