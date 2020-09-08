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

import org.apache.samza.application.ApplicationUtil;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.loaders.PropertiesConfigLoaderFactory;
import org.apache.samza.util.ConfigUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;


@RunWith(PowerMockRunner.class)
@PrepareForTest({
    ApplicationUtil.class,
    ConfigUtil.class,
    JobCoordinatorLaunchUtil.class,
    ClusterBasedJobCoordinatorRunner.class})
public class TestDefaultApplicationMain {

  @Test
  public void testRun() throws Exception {
    String[] args = new String[] {
        "--config",
        JobConfig.CONFIG_LOADER_FACTORY + "=" + PropertiesConfigLoaderFactory.class.getName(),
        "--config",
        PropertiesConfigLoaderFactory.CONFIG_LOADER_PROPERTIES_PREFIX + "path=" + getClass().getResource("/test.properties").getPath()
    };

    StreamApplication mockApplication = mock(StreamApplication.class);
    Config mockConfig = mock(Config.class);
    mockStatic(JobCoordinatorLaunchUtil.class, ApplicationUtil.class, ConfigUtil.class);

    when(ApplicationUtil.fromConfig(any()))
        .thenReturn(mockApplication);
    when(ConfigUtil.loadConfig(any()))
        .thenReturn(mockConfig);
    doNothing()
        .when(JobCoordinatorLaunchUtil.class, "run",
            mockApplication, mockConfig);
    DefaultApplicationMain.run(args);

    verifyStatic(times(1));
    JobCoordinatorLaunchUtil.run(mockApplication, mockConfig);
  }
}
