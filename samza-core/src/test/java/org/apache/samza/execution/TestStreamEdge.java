package org.apache.samza.execution;

import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.system.StreamSpec;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestStreamEdge {
  StreamSpec spec = new StreamSpec("stream-1", "physical-stream-1", "system-1");

  @Test
  public void testGetStreamSpec() {
    StreamEdge edge = new StreamEdge(spec, false, new MapConfig());
    assertEquals(edge.getStreamSpec(), spec);
    assertEquals(edge.getStreamSpec().getPartitionCount(), 1 /*StreamSpec.DEFAULT_PARTITION_COUNT*/);

    edge.setPartitionCount(10);
    assertEquals(edge.getStreamSpec().getPartitionCount(), 10);
  }

  @Test
  public void testGetStreamSpec_Batch() {
    Map<String, String> config = new HashMap<>();
    config.put(ApplicationConfig.APP_MODE, ApplicationConfig.ApplicationMode.BATCH.name());
    config.put(ApplicationConfig.APP_RUN_ID, "123");
    StreamEdge edge = new StreamEdge(spec, true, new MapConfig(config));
    assertEquals(edge.getStreamSpec().getPhysicalName(), spec.getPhysicalName() + "-123");
  }

  @Test
  public void testGenerateConfig() {
    // an example unbounded IO stream
    StreamSpec spec = new StreamSpec("stream-1", "physical-stream-1", "system-1", false, Collections.singletonMap("property1", "haha"));
    StreamEdge edge = new StreamEdge(spec, false, new MapConfig());
    Config config = edge.generateConfig();
    StreamConfig streamConfig = new StreamConfig(config);
    assertEquals(streamConfig.getSystem(spec.getId()), "system-1");
    assertEquals(streamConfig.getPhysicalName(spec.getId()), "physical-stream-1");
    assertEquals(streamConfig.getIsIntermediate(spec.getId()), false);
    assertEquals(streamConfig.getIsBounded(spec.getId()), false);
    assertEquals(streamConfig.getStreamProperties(spec.getId()).get("property1"), "haha");

    // bounded stream
    spec = new StreamSpec("stream-1", "physical-stream-1", "system-1", true, Collections.singletonMap("property1", "haha"));
    edge = new StreamEdge(spec, false, new MapConfig());
    config = edge.generateConfig();
    streamConfig = new StreamConfig(config);
    assertEquals(streamConfig.getIsBounded(spec.getId()), true);

    // intermediate stream
    edge = new StreamEdge(spec, true, new MapConfig());
    config = edge.generateConfig();
    streamConfig = new StreamConfig(config);
    assertEquals(streamConfig.getIsIntermediate(spec.getId()), true);
    assertEquals(streamConfig.getDefaultStreamOffset(spec.toSystemStream()).get(), "oldest");
  }
}
