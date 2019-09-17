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

package org.apache.samza.runtime;

import com.google.common.collect.ImmutableMap;
import org.apache.samza.SamzaException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;


public class TestClusterBasedProcessorLifecycleListener {
  private ClusterBasedProcessorLifecycleListener clusterBasedProcessorLifecycleListener;
  private ProcessorLifecycleListener processorLifecycleListener;
  private Runnable mockShutdownHookCallback;

  @Before
  public void setup() {
    mockShutdownHookCallback = mock(Runnable.class);
    processorLifecycleListener = mock(ProcessorLifecycleListener.class);
    clusterBasedProcessorLifecycleListener =
        spy(new ClusterBasedProcessorLifecycleListener(new MapConfig(ImmutableMap.of(TaskConfig.TASK_SHUTDOWN_MS, "1")),
            processorLifecycleListener, mockShutdownHookCallback));
    doNothing().when(clusterBasedProcessorLifecycleListener).addJVMShutdownHook(any(Thread.class));
    doNothing().when(clusterBasedProcessorLifecycleListener).removeJVMShutdownHook(any(Thread.class));
  }

  @Test
  public void testLifecycleListenerBeforeStart() {
    clusterBasedProcessorLifecycleListener.beforeStart();

    Mockito.verify(clusterBasedProcessorLifecycleListener).addJVMShutdownHook(any(Thread.class));
    Mockito.verify(processorLifecycleListener).beforeStart();
  }

  @Test
  public void testLifecycleListenerAfterStart() {
    clusterBasedProcessorLifecycleListener.afterStart();
    Mockito.verify(processorLifecycleListener).afterStart();
  }

  @Test
  public void testLifecycleListenerAfterStop() {
    clusterBasedProcessorLifecycleListener.afterStop();
    Mockito.verify(processorLifecycleListener).afterStop();
    Mockito.verify(clusterBasedProcessorLifecycleListener).removeJVMShutdownHook(any(Thread.class));
  }

  @Test
  public void testLifecycleListenerAfterFailure() {
    SamzaException e = new SamzaException("Should call afterFailure");
    clusterBasedProcessorLifecycleListener.afterFailure(e);
    Mockito.verify(processorLifecycleListener).afterFailure(e);
    Mockito.verify(clusterBasedProcessorLifecycleListener).removeJVMShutdownHook(any(Thread.class));
  }

  @Test
  public void testShutdownHookInvokesShutdownHookCallback() {
    doNothing().when(mockShutdownHookCallback).run();

    // setup shutdown hook
    clusterBasedProcessorLifecycleListener.beforeStart();

    // Simulating shutdown hook invocation by JVM
    clusterBasedProcessorLifecycleListener.getShutdownHookThread().run();
    Mockito.verify(mockShutdownHookCallback).run();
  }
}
