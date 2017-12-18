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

package org.apache.samza.coordinator;

import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.grouper.task.TaskAssignmentManager;
import org.apache.samza.coordinator.server.HttpServer;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.testUtils.MockHttpServer;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.mockito.ArgumentMatcher;
import scala.collection.JavaConversions;

/**
 * Unit tests for {@link JobModelManager}
 */
public class TestJobModelManager {
  private final TaskAssignmentManager mockTaskManager = mock(TaskAssignmentManager.class);
  private final LocalityManager mockLocalityManager = mock(LocalityManager.class);
  private final Map<String, Map<String, String>> localityMappings = new HashMap<>();
  private final HttpServer server = new MockHttpServer("/", 7777, null, new ServletHolder(DefaultServlet.class));
  private final SystemStream inputStream = new SystemStream("test-system", "test-stream");
  private final SystemStreamMetadata.SystemStreamPartitionMetadata mockSspMetadata = mock(SystemStreamMetadata.SystemStreamPartitionMetadata.class);
  private final Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> mockSspMetadataMap = Collections.singletonMap(new Partition(0), mockSspMetadata);
  private final SystemStreamMetadata mockStreamMetadata = mock(SystemStreamMetadata.class);
  private final scala.collection.immutable.Map<SystemStream, SystemStreamMetadata> mockStreamMetadataMap = new scala.collection.immutable.Map.Map1(inputStream, mockStreamMetadata);
  private final StreamMetadataCache mockStreamMetadataCache = mock(StreamMetadataCache.class);
  private final scala.collection.immutable.Set<SystemStream> inputStreamSet = JavaConversions.asScalaSet(Collections.singleton(inputStream)).toSet();

  private JobModelManager jobModelManager;

  @Before
  public void setup() {
    when(mockLocalityManager.readContainerLocality()).thenReturn(this.localityMappings);
    when(mockStreamMetadataCache.getStreamMetadata(argThat(new ArgumentMatcher<scala.collection.immutable.Set<SystemStream>>() {
      @Override
      public boolean matches(Object argument) {
        scala.collection.immutable.Set<SystemStream> set = (scala.collection.immutable.Set<SystemStream>) argument;
        return set.equals(inputStreamSet);
      }
    }), anyBoolean())).thenReturn(mockStreamMetadataMap);
    when(mockStreamMetadata.getSystemStreamPartitionMetadata()).thenReturn(mockSspMetadataMap);
    when(mockLocalityManager.getTaskAssignmentManager()).thenReturn(mockTaskManager);
    when(mockTaskManager.readTaskAssignment()).thenReturn(Collections.EMPTY_MAP);
  }

  @Test
  public void testLocalityMapWithHostAffinity() {
    Config config = new MapConfig(new HashMap<String, String>() {
      {
        put("cluster-manager.container.count", "1");
        put("cluster-manager.container.memory.mb", "512");
        put("cluster-manager.container.retry.count", "1");
        put("cluster-manager.container.retry.window.ms", "1999999999");
        put("cluster-manager.allocator.sleep.ms", "10");
        put("yarn.package.path", "/foo");
        put("task.inputs", "test-system.test-stream");
        put("systems.test-system.samza.factory", "org.apache.samza.system.MockSystemFactory");
        put("systems.test-system.samza.key.serde", "org.apache.samza.serializers.JsonSerde");
        put("systems.test-system.samza.msg.serde", "org.apache.samza.serializers.JsonSerde");
        put("job.host-affinity.enabled", "true");
      }
    });

    this.localityMappings.put("0", new HashMap<String, String>() { {
        put(SetContainerHostMapping.HOST_KEY, "abc-affinity");
      } });
    this.jobModelManager = JobModelManagerTestUtil.getJobModelManagerUsingReadModel(config, 1, mockStreamMetadataCache, mockLocalityManager, server);

    assertEquals(jobModelManager.jobModel().getAllContainerLocality(), new HashMap<String, String>() { { this.put("0", "abc-affinity"); } });
  }

  @Test
  public void testLocalityMapWithoutHostAffinity() {
    Config config = new MapConfig(new HashMap<String, String>() {
      {
        put("cluster-manager.container.count", "1");
        put("cluster-manager.container.memory.mb", "512");
        put("cluster-manager.container.retry.count", "1");
        put("cluster-manager.container.retry.window.ms", "1999999999");
        put("cluster-manager.allocator.sleep.ms", "10");
        put("yarn.package.path", "/foo");
        put("task.inputs", "test-system.test-stream");
        put("systems.test-system.samza.factory", "org.apache.samza.system.MockSystemFactory");
        put("systems.test-system.samza.key.serde", "org.apache.samza.serializers.JsonSerde");
        put("systems.test-system.samza.msg.serde", "org.apache.samza.serializers.JsonSerde");
        put("job.host-affinity.enabled", "false");
      }
    });

    this.localityMappings.put("0", new HashMap<String, String>() { {
        put(SetContainerHostMapping.HOST_KEY, "abc-affinity");
      } });
    this.jobModelManager = JobModelManagerTestUtil.getJobModelManagerUsingReadModel(config, 1, mockStreamMetadataCache, mockLocalityManager, server);

    assertEquals(jobModelManager.jobModel().getAllContainerLocality(), new HashMap<String, String>() { { this.put("0", null); } });
  }
}
