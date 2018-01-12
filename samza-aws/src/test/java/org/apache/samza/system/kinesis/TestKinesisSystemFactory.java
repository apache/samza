/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.system.kinesis;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.grouper.stream.AllSspToSingleTaskGrouperFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;


public class TestKinesisSystemFactory {
  private static final String SYSTEM_FACTORY_REGEX = "systems.%s.samza.factory";
  private static final String KINESIS_SYSTEM_FACTORY = KinesisSystemFactory.class.getName();

  @Test
  public void testGetConsumer() {
    String systemName = "test";
    Config config = buildKinesisConsumerConfig(systemName);
    KinesisSystemFactory factory = new KinesisSystemFactory();
    MetricsRegistry metricsRegistry = new NoOpMetricsRegistry();
    Assert.assertNotSame(factory.getConsumer("test", config, metricsRegistry), factory.getAdmin(systemName, config));
  }

  @Ignore
  @Test(expected = ConfigException.class)
  public void testGetAdminWithIncorrectSspGrouper() {
    String systemName = "test";
    KinesisSystemFactory factory = new KinesisSystemFactory();
    Config config = buildKinesisConsumerConfig(systemName,
        "org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouperFactory");
    factory.getAdmin(systemName, config);
  }

  @Ignore
  @Test(expected = ConfigException.class)
  public void testGetAdminWithBroadcastStreams() {
    String systemName = "test";
    KinesisSystemFactory factory = new KinesisSystemFactory();
    Config config = buildKinesisConsumerConfig(systemName,
        "org.apache.samza.container.grouper.stream.AllSspToSingleTaskGrouperFactory",
        "test.stream#0");
    factory.getAdmin(systemName, config);
  }

  @Ignore
  @Test(expected = ConfigException.class)
  public void testGetAdminWithBootstrapStream() {
    String systemName = "test";
    KinesisSystemFactory factory = new KinesisSystemFactory();
    Config config = buildKinesisConsumerConfig(systemName,
        "org.apache.samza.container.grouper.stream.AllSspToSingleTaskGrouperFactory",
        null,
        "kinesis-stream"
        );
    factory.getAdmin(systemName, config);
  }

  private static Config buildKinesisConsumerConfig(String systemName) {
    return buildKinesisConsumerConfig(systemName, AllSspToSingleTaskGrouperFactory.class.getCanonicalName());
  }

  private static Config buildKinesisConsumerConfig(String systemName, String sspGrouperFactory) {
    return buildKinesisConsumerConfig(systemName, sspGrouperFactory, null);
  }

  private static Config buildKinesisConsumerConfig(String systemName, String sspGrouperFactory,
      String broadcastStreamConfigValue) {
    return buildKinesisConsumerConfig(systemName, sspGrouperFactory, broadcastStreamConfigValue, null);
  }

  private static Config buildKinesisConsumerConfig(String systemName, String sspGrouperFactory,
      String broadcastStreamConfigValue, String bootstrapStreamName) {
    Map<String, String> props = buildSamzaKinesisSystemConfig(systemName, sspGrouperFactory, broadcastStreamConfigValue,
        bootstrapStreamName);
    return new MapConfig(props);
  }

  private static Map<String, String> buildSamzaKinesisSystemConfig(String systemName, String sspGrouperFactory,
      String broadcastStreamConfigValue, String bootstrapStreamName) {
    Map<String, String> result = new HashMap<>();
    result.put(String.format(SYSTEM_FACTORY_REGEX, systemName), KINESIS_SYSTEM_FACTORY);
    result.put("job.systemstreampartition.grouper.factory", sspGrouperFactory);
    if (broadcastStreamConfigValue != null && !broadcastStreamConfigValue.isEmpty()) {
      result.put("task.broadcast.inputs", broadcastStreamConfigValue);
    }
    if (bootstrapStreamName != null && !bootstrapStreamName.isEmpty()) {
      result.put("systems." + systemName + ".streams." + bootstrapStreamName + ".samza.bootstrap", "true");
    }
    return result;
  }
}
