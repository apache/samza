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

import com.google.common.collect.ImmutableMap;
import org.apache.samza.application.ApplicationUtil;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.loaders.PropertiesConfigLoaderFactory;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.util.ConfigUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.mock;


@RunWith(PowerMockRunner.class)
@PrepareForTest({
    System.class,
    ClusterBasedJobCoordinatorRunner.class,
    ApplicationUtil.class,
    JobCoordinatorLaunchUtil.class
})
public class TestClusterBasedJobCoordinatorRunner {

  @Test
  public void testRunClusterBasedJobCoordinator() throws Exception  {
    Config submissionConfig = new MapConfig(ImmutableMap.of(
        JobConfig.CONFIG_LOADER_FACTORY,
        PropertiesConfigLoaderFactory.class.getName(),
        PropertiesConfigLoaderFactory.CONFIG_LOADER_PROPERTIES_PREFIX + "path",
        getClass().getResource("/test.properties").getPath()));
    Config fullConfig = ConfigUtil.loadConfig(submissionConfig);
    StreamApplication mockApplication = mock(StreamApplication.class);
    PowerMockito.mockStatic(System.class, ApplicationUtil.class, JobCoordinatorLaunchUtil.class);
    PowerMockito
        .when(System.getenv(eq(ShellCommandConfig.ENV_SUBMISSION_CONFIG)))
        .thenReturn(SamzaObjectMapper.getObjectMapper().writeValueAsString(submissionConfig));
    PowerMockito
        .when(ApplicationUtil.fromConfig(any()))
        .thenReturn(mockApplication);
    PowerMockito.doNothing().when(JobCoordinatorLaunchUtil.class, "run", mockApplication, fullConfig);

    ClusterBasedJobCoordinatorRunner.runClusterBasedJobCoordinator(null);

    PowerMockito.verifyStatic(times(1));
    JobCoordinatorLaunchUtil.run(mockApplication, fullConfig);
  }
}
