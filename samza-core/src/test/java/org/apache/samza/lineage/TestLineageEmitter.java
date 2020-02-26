package org.apache.samza.lineage;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.LineageConfig;
import org.apache.samza.config.MapConfig;
import org.junit.Test;


public class TestLineageEmitter {

  @Test(expected = ConfigException.class)
  public void testEmitWhenNotConfigLineageFactory() {
    Map<String, String> configs = new HashMap<>();
    configs.put(LineageConfig.LINEAGE_REPORTER_FACTORY, "org.apache.samza.lineage.mock.MockLineageReporterFactory");
    LineageEmitter.emit(new MapConfig(configs));
  }

  @Test(expected = ConfigException.class)
  public void testEmitWhenNotConfigLineageReporterFactory() {
    Map<String, String> configs = new HashMap<>();
    configs.put(LineageConfig.LINEAGE_FACTORY, "org.apache.samza.lineage.mock.MockLineageFactory");
    LineageEmitter.emit(new MapConfig(configs));
  }

  @Test
  public void testEmitWhenConfigNeitherOfLineageFactoryAndLineageRepoterFactory() {
    Map<String, String> configs = new HashMap<>();
    LineageEmitter.emit(new MapConfig(configs));
  }

  @Test
  public void testEmit() {
    Map<String, String> configs = new HashMap<>();
    configs.put(LineageConfig.LINEAGE_FACTORY, "org.apache.samza.lineage.mock.MockLineageFactory");
    configs.put(LineageConfig.LINEAGE_REPORTER_FACTORY, "org.apache.samza.lineage.mock.MockLineageReporterFactory");
    LineageEmitter.emit(new MapConfig(configs));
  }

}
