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

package org.apache.samza.context;

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;

import java.util.Collections;

import static org.mockito.Mockito.*;


public class MockContext implements Context {
  private final JobContext jobContext = mock(JobContext.class);
  private final ContainerContext containerContext = mock(ContainerContext.class);
  /**
   * This is {@link TaskContextImpl} because some tests need more than just the interface.
   */
  private final TaskContextImpl taskContext = mock(TaskContextImpl.class);
  private final ApplicationContainerContext applicationContainerContext = mock(ApplicationContainerContext.class);
  private final ApplicationTaskContext applicationTaskContext = mock(ApplicationTaskContext.class);
  private final ExternalContext externalContext = mock(ExternalContext.class);

  public MockContext() {
    this(new MapConfig(
        Collections.singletonMap("metrics.timer.debug.enabled", "true")
    ));
  }

  /**
   * @param config config is widely used, so help wire it in here
   */
  public MockContext(Config config) {
    when(this.jobContext.getConfig()).thenReturn(config);
  }

  @Override
  public JobContext getJobContext() {
    return this.jobContext;
  }

  @Override
  public ContainerContext getContainerContext() {
    return this.containerContext;
  }

  @Override
  public TaskContext getTaskContext() {
    return this.taskContext;
  }

  @Override
  public ApplicationContainerContext getApplicationContainerContext() {
    return this.applicationContainerContext;
  }

  @Override
  public ApplicationTaskContext getApplicationTaskContext() {
    return this.applicationTaskContext;
  }

  @Override
  public ExternalContext getExternalContext() {
    return this.externalContext;
  }
}
