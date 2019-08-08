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

import com.google.common.collect.ImmutableMap;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


public class TestMetadataResourceUtil {
  private JobModel mockJobModel;
  private MetricsRegistry mockMetricsRegistry;

  @Before
  public void setUp() {
    mockJobModel = Mockito.mock(JobModel.class);
    mockMetricsRegistry = Mockito.mock(MetricsRegistry.class);
  }

  @Test
  public void testLoadWithCheckpointConfigured() {
    MapConfig mapConfig = new MapConfig(ImmutableMap.of(TaskConfig.CHECKPOINT_MANAGER_FACTORY,
        TestCheckpointManagerFactory.class.getName()));
    MetadataResourceUtil metadataResourceUtil = Mockito.spy(new MetadataResourceUtil(mockJobModel, mockMetricsRegistry, getClass().getClassLoader(), mapConfig));
    Mockito.doNothing().when(metadataResourceUtil).createChangelogStreams();

    metadataResourceUtil.createResources();
    Mockito.verify(mockJobModel, Mockito.never()).getConfig(); // Never get config from job model. In standalone, this is empty.
    Mockito.verify(metadataResourceUtil.getCheckpointManager()).createResources();
    Mockito.verify(metadataResourceUtil).createChangelogStreams();
  }


  @Test
  public void testLoadWithoutCheckpointConfigured() {
    MapConfig mapConfig = new MapConfig();
    MetadataResourceUtil metadataResourceUtil = Mockito.spy(new MetadataResourceUtil(mockJobModel, mockMetricsRegistry, getClass().getClassLoader(), mapConfig));
    Mockito.doNothing().when(metadataResourceUtil).createChangelogStreams();

    metadataResourceUtil.createResources();
    Mockito.verify(mockJobModel, Mockito.never()).getConfig(); // Never get config from job model. In standalone, this is empty.
    Assert.assertNull(metadataResourceUtil.getCheckpointManager());
    Mockito.verify(metadataResourceUtil).createChangelogStreams();
  }

  public static class TestCheckpointManagerFactory implements CheckpointManagerFactory {
    @Override
    public CheckpointManager getCheckpointManager(Config config, MetricsRegistry registry) {
      return Mockito.mock(CheckpointManager.class);
    }
  }
}