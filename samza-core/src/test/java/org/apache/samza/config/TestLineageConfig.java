package org.apache.samza.config;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestLineageConfig {

  @Test
  public void testGetLineageFactoryClassName() {
    String expectedLineageFactory = "org.apache.samza.MockLineageFactory";
    MapConfig config =
        new MapConfig(ImmutableMap.of(LineageConfig.LINEAGE_FACTORY, expectedLineageFactory));
    LineageConfig lineageConfig = new LineageConfig(config);
    assertTrue(lineageConfig.getLineageFactoryClassName().isPresent());
    assertEquals(expectedLineageFactory, lineageConfig.getLineageFactoryClassName().get());
  }

  @Test
  public void testGetLineageReporterFactoryClassName() {
    String expectedLineageReporterFactory = "org.apache.samza.MockLineageReporterFactory";
    MapConfig config =
        new MapConfig(ImmutableMap.of(LineageConfig.LINEAGE_REPORTER_FACTORY, expectedLineageReporterFactory));
    LineageConfig lineageConfig = new LineageConfig(config);
    assertTrue(lineageConfig.getLineageReporterFactoryClassName().isPresent());
    assertEquals(expectedLineageReporterFactory, lineageConfig.getLineageReporterFactoryClassName().get());
  }

  @Test
  public void testGetLineageReporterStreamName() {
    String expectedLineageReporterStream = "kafka.foo";
    MapConfig config =
        new MapConfig(ImmutableMap.of(LineageConfig.LINEAGE_REPORTER_STREAM, expectedLineageReporterStream));
    LineageConfig lineageConfig = new LineageConfig(config);
    assertTrue(lineageConfig.getLineageReporterStreamName().isPresent());
    assertEquals(expectedLineageReporterStream, lineageConfig.getLineageReporterStreamName().get());
  }
}
