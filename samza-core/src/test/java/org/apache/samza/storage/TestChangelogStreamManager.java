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
package org.apache.samza.storage;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.StreamValidationException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestChangelogStreamManager {
  private static final String SYSTEM = "system";
  private static final String STREAM = "stream";
  private static final int MAX_CHANGELOG_STREAM_PARTITIONS = 10;
  private static final Set<StreamSpec> EXPECTED_STREAM_SPECS = ImmutableSet.of(
      // changelog
      StreamSpec.createChangeLogStreamSpec(STREAM, SYSTEM, MAX_CHANGELOG_STREAM_PARTITIONS),
      // access log
      new StreamSpec(STREAM + "-access-log", STREAM + "-access-log", SYSTEM, MAX_CHANGELOG_STREAM_PARTITIONS));

  @Test
  public void createChangelogStreams() {
    Map<String, String> map = new ImmutableMap.Builder<String, String>()
        .put("stores.store0.factory", "factory.class")
        .put("stores.store0.changelog", SYSTEM + "." + STREAM)
        .put("stores.store0.accesslog.enabled", "true")
        .put(String.format("systems.%s.samza.factory", SYSTEM), MockSystemAdminFactory.class.getName())
        .build();
    Config config = new MapConfig(map);
    ChangelogStreamManager.createChangelogStreams(config, MAX_CHANGELOG_STREAM_PARTITIONS);
  }

  public static class MockSystemAdminFactory implements SystemFactory {
    @Override
    public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
      throw new UnsupportedOperationException("Unused in test");
    }

    @Override
    public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
      throw new UnsupportedOperationException("Unused in test");
    }

    @Override
    public SystemAdmin getAdmin(String systemName, Config config) {
      return new MockSystemAdmin();
    }
  }

  private static class MockSystemAdmin implements SystemAdmin {
    private final Set<StreamSpec> streamsCreated = new HashSet<>();
    private final Set<StreamSpec> streamsValidated = new HashSet<>();

    @Override
    public boolean createStream(StreamSpec streamSpec) {
      assertTrue(EXPECTED_STREAM_SPECS.contains(streamSpec));
      this.streamsCreated.add(streamSpec);
      return true;
    }

    @Override
    public void validateStream(StreamSpec streamSpec) {
      if (!EXPECTED_STREAM_SPECS.contains(streamSpec)) {
        throw new StreamValidationException("Did not see expected stream spec");
      }
      this.streamsValidated.add(streamSpec);
    }

    @Override
    public void stop() {
      assertEquals(EXPECTED_STREAM_SPECS, this.streamsCreated);
      assertEquals(EXPECTED_STREAM_SPECS, this.streamsValidated);
    }

    @Override
    public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
      throw new UnsupportedOperationException("Unused in test");
    }

    @Override
    public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
      throw new UnsupportedOperationException("Unused in test");
    }

    @Override
    public Integer offsetComparator(String offset1, String offset2) {
      throw new UnsupportedOperationException("Unused in test");
    }
  }
}
