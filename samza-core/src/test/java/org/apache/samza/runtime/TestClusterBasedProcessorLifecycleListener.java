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
import org.apache.samza.container.RunLoop;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.processor.StreamProcessorTestUtils;
import org.apache.samza.task.StreamTask;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;


public class TestClusterBasedProcessorLifecycleListener {

  private SamzaContainer container;
  private RunLoop mockRunLoop;
  private ClusterBasedProcessorLifecycleListener clusterBasedProcessorLifecycleListener;
  private ProcessorLifecycleListener processorLifecycleListener;
  private Runnable mockOnSignal;

  @Before
  public void setup() {
    mockRunLoop = mock(RunLoop.class);
    mockOnSignal = mock(Runnable.class);
    container = StreamProcessorTestUtils.getDummyContainer(mockRunLoop, Mockito.mock(StreamTask.class));
    processorLifecycleListener = mock(ProcessorLifecycleListener.class);
    clusterBasedProcessorLifecycleListener =
        spy(new ClusterBasedProcessorLifecycleListener(new MapConfig(ImmutableMap.of(TaskConfig.TASK_SHUTDOWN_MS, "1")),
            processorLifecycleListener, mockOnSignal));
    container.setContainerListener(clusterBasedProcessorLifecycleListener);
    doNothing().when(clusterBasedProcessorLifecycleListener).addJVMShutdownHook();
    doNothing().when(clusterBasedProcessorLifecycleListener).removeJVMShutdownHook();
  }

  @Test
  public void testProcessorLifecycleListenerIsCalledOnContainerShutdown() {
    container.run();
    container.shutdown();

    Mockito.verify(clusterBasedProcessorLifecycleListener, Mockito.times(1)).addJVMShutdownHook();
    Mockito.verify(clusterBasedProcessorLifecycleListener, Mockito.times(1)).removeJVMShutdownHook();
    Mockito.verify(processorLifecycleListener, Mockito.times(1)).beforeStart();
    Mockito.verify(processorLifecycleListener, Mockito.times(1)).afterStart();
    Mockito.verify(processorLifecycleListener, Mockito.times(1)).afterStop();
    Mockito.verify(processorLifecycleListener, Mockito.times(0)).afterFailure(any(Throwable.class));
  }

  @Test
  public void testProcessorLifecycleListenerIsCalledOnContainerError() {
    SamzaException e = new SamzaException("Should call afterFailure");
    doAnswer(invocation -> {
        throw e;
      }).when(mockRunLoop).run();

    container.run();

    Mockito.verify(clusterBasedProcessorLifecycleListener, Mockito.times(1)).addJVMShutdownHook();
    Mockito.verify(clusterBasedProcessorLifecycleListener, Mockito.times(1)).removeJVMShutdownHook();
    Mockito.verify(processorLifecycleListener, Mockito.times(1)).beforeStart();
    Mockito.verify(processorLifecycleListener, Mockito.times(1)).afterStart();
    Mockito.verify(processorLifecycleListener, Mockito.times(0)).afterStop();
    Mockito.verify(processorLifecycleListener, Mockito.times(1)).afterFailure(e);
  }

  @Test
  public void testShutdownHookInvokesOnSignalCallback() {
    doNothing().when(mockOnSignal).run();
    container.run();

    // Simulating signal handler invocation by JVM
    clusterBasedProcessorLifecycleListener.getShutdownHookThread().run();
    Mockito.verify(mockOnSignal, Mockito.times(1)).run();
  }
}
