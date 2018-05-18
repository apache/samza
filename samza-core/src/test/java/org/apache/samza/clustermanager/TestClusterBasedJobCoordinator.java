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

package org.apache.samza.clustermanager;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.StreamPartitionCountMonitor;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemProducer;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.MockSystemFactory;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;


/**
 * Tests for {@link ClusterBasedJobCoordinator}
 */
public class TestClusterBasedJobCoordinator {

  Map<String, String> configMap;

  @Before
  public void setUp() throws NoSuchFieldException, NoSuchMethodException {
    configMap = new HashMap<>();
    configMap.put("job.name", "test-job");
    configMap.put("job.coordinator.system", "kafka");
    configMap.put("task.inputs", "kafka.topic1");
    configMap.put("systems.kafka.samza.factory", "org.apache.samza.system.MockSystemFactory");
    configMap.put("samza.cluster-manager.factory", "org.apache.samza.clustermanager.MockClusterResourceManagerFactory");
    configMap.put("job.coordinator.monitor-partition-change.frequency.ms", "1");

    MockSystemFactory.MSG_QUEUES.put(new SystemStreamPartition("kafka", "topic1", new Partition(0)), new ArrayList<>());
  }

  @After
  public void tearDown() {
    MockSystemFactory.MSG_QUEUES.clear();
  }

  @Test
  public void testPartitionCountMonitorWithDurableStates()
      throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    configMap.put("stores.mystore.changelog", "mychangelog");
    Config config = new MapConfig(configMap);

    // mimic job runner code to write the config to coordinator stream
    CoordinatorStreamSystemProducer producer = new CoordinatorStreamSystemProducer(config, mock(MetricsRegistry.class));
    producer.writeConfig("test-job", config);

    ClusterBasedJobCoordinator clusterCoordinator = new ClusterBasedJobCoordinator(config);

    // change the input system stream metadata
    MockSystemFactory.MSG_QUEUES.put(new SystemStreamPartition("kafka", "topic1", new Partition(1)), new ArrayList<>());

    StreamPartitionCountMonitor monitor = clusterCoordinator.getPartitionMonitor();
    monitor.updatePartitionCountMetric();
    assertEquals(clusterCoordinator.getAppStatus(), SamzaApplicationState.SamzaAppStatus.FAILED);
  }

  @Test
  public void testPartitionCountMonitorWithoutDurableStates() throws IllegalAccessException, InvocationTargetException {
    Config config = new MapConfig(configMap);

    // mimic job runner code to write the config to coordinator stream
    CoordinatorStreamSystemProducer producer = new CoordinatorStreamSystemProducer(config, mock(MetricsRegistry.class));
    producer.writeConfig("test-job", config);

    ClusterBasedJobCoordinator clusterCoordinator = new ClusterBasedJobCoordinator(config);

    // change the input system stream metadata
    MockSystemFactory.MSG_QUEUES.put(new SystemStreamPartition("kafka", "topic1", new Partition(1)), new ArrayList<>());

    StreamPartitionCountMonitor monitor = clusterCoordinator.getPartitionMonitor();
    monitor.updatePartitionCountMetric();
    assertEquals(clusterCoordinator.getAppStatus(), SamzaApplicationState.SamzaAppStatus.UNDEFINED);
  }

}
