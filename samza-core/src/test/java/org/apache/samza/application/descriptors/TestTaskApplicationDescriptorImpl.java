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
package org.apache.samza.application.descriptors;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.config.Config;
import org.apache.samza.context.ApplicationContainerContextFactory;
import org.apache.samza.context.ApplicationTaskContextFactory;
import org.apache.samza.system.descriptors.InputDescriptor;
import org.apache.samza.system.descriptors.OutputDescriptor;
import org.apache.samza.table.descriptors.BaseTableDescriptor;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.system.descriptors.SystemDescriptor;
import org.apache.samza.runtime.ProcessorLifecycleListenerFactory;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.task.TaskFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Unit test for {@link TaskApplicationDescriptorImpl}
 */
public class TestTaskApplicationDescriptorImpl {

  private Config config = mock(Config.class);
  private String defaultSystemName = "test-system";
  private SystemDescriptor defaultSystemDescriptor = mock(SystemDescriptor.class);
  private List<InputDescriptor> mockInputs = new ArrayList<InputDescriptor>() { {
      InputDescriptor mock1 = mock(InputDescriptor.class);
      InputDescriptor mock2 = mock(InputDescriptor.class);
      when(mock1.getStreamId()).thenReturn("test-input1");
      when(mock2.getStreamId()).thenReturn("test-input2");
      this.add(mock1);
      this.add(mock2);
    } };
  private List<OutputDescriptor> mockOutputs = new ArrayList<OutputDescriptor>() { {
      OutputDescriptor mock1 = mock(OutputDescriptor.class);
      OutputDescriptor mock2 = mock(OutputDescriptor.class);
      when(mock1.getStreamId()).thenReturn("test-output1");
      when(mock2.getStreamId()).thenReturn("test-output2");
      this.add(mock1);
      this.add(mock2);
    } };
  private Set<TableDescriptor> mockTables = new HashSet<TableDescriptor>() { {
      BaseTableDescriptor mock1 = mock(BaseTableDescriptor.class);
      BaseTableDescriptor mock2 = mock(BaseTableDescriptor.class);
      when(mock1.getTableId()).thenReturn("test-table1");
      when(mock2.getTableId()).thenReturn("test-table2");
      when(mock1.getSerde()).thenReturn(mock(KVSerde.class));
      when(mock2.getSerde()).thenReturn(mock(KVSerde.class));
      this.add(mock1);
      this.add(mock2);
    } };

  @Before
  public void setUp() {
    when(defaultSystemDescriptor.getSystemName()).thenReturn(defaultSystemName);
    mockInputs.forEach(isd -> when(isd.getSystemDescriptor()).thenReturn(defaultSystemDescriptor));
    mockOutputs.forEach(osd -> when(osd.getSystemDescriptor()).thenReturn(defaultSystemDescriptor));
  }

  @Test
  public void testConstructor() {
    TaskApplication mockApp = mock(TaskApplication.class);
    TaskApplicationDescriptorImpl appDesc = new TaskApplicationDescriptorImpl(mockApp, config);
    verify(mockApp).describe(appDesc);
    assertEquals(config, appDesc.getConfig());
  }

  @Test
  public void testAddInputStreams() {
    TaskApplication testApp = appDesc -> {
      mockInputs.forEach(appDesc::addInputStream);
    };
    TaskApplicationDescriptorImpl appDesc = new TaskApplicationDescriptorImpl(testApp, config);
    assertEquals(mockInputs.toArray(), appDesc.getInputDescriptors().values().toArray());
  }

  @Test
  public void testAddOutputStreams() {
    TaskApplication testApp = appDesc -> {
      mockOutputs.forEach(appDesc::addOutputStream);
    };
    TaskApplicationDescriptorImpl appDesc = new TaskApplicationDescriptorImpl(testApp, config);
    assertEquals(mockOutputs.toArray(), appDesc.getOutputDescriptors().values().toArray());
  }

  @Test
  public void testAddTables() {
    TaskApplication testApp = appDesc -> {
      mockTables.forEach(appDesc::addTable);
    };
    TaskApplicationDescriptorImpl appDesc = new TaskApplicationDescriptorImpl(testApp, config);
    assertEquals(mockTables, appDesc.getTableDescriptors());
  }

  @Test
  public void testWithTaskFactory() {
    TaskFactory mockTf = mock(TaskFactory.class);
    TaskApplication testApp = appDesc -> appDesc.withTaskFactory(mockTf);
    TaskApplicationDescriptorImpl appDesc = new TaskApplicationDescriptorImpl(testApp, config);
    assertEquals(appDesc.getTaskFactory(), mockTf);
  }

  @Test
  public void testApplicationContainerContextFactory() {
    ApplicationContainerContextFactory factory = mock(ApplicationContainerContextFactory.class);
    TaskApplication testApp = appDesc -> appDesc.withApplicationContainerContextFactory(factory);
    TaskApplicationDescriptorImpl appSpec = new TaskApplicationDescriptorImpl(testApp, mock(Config.class));
    assertEquals(appSpec.getApplicationContainerContextFactory(), Optional.of(factory));
  }

  @Test
  public void testNoApplicationContainerContextFactory() {
    TaskApplication testApp = appDesc -> {
    };
    TaskApplicationDescriptorImpl appSpec = new TaskApplicationDescriptorImpl(testApp, mock(Config.class));
    assertEquals(appSpec.getApplicationContainerContextFactory(), Optional.empty());
  }

  @Test
  public void testApplicationTaskContextFactory() {
    ApplicationTaskContextFactory factory = mock(ApplicationTaskContextFactory.class);
    TaskApplication testApp = appDesc -> appDesc.withApplicationTaskContextFactory(factory);
    TaskApplicationDescriptorImpl appSpec = new TaskApplicationDescriptorImpl(testApp, mock(Config.class));
    assertEquals(appSpec.getApplicationTaskContextFactory(), Optional.of(factory));
  }

  @Test
  public void testNoApplicationTaskContextFactory() {
    TaskApplication testApp = appDesc -> {
    };
    TaskApplicationDescriptorImpl appSpec = new TaskApplicationDescriptorImpl(testApp, mock(Config.class));
    assertEquals(appSpec.getApplicationTaskContextFactory(), Optional.empty());
  }

  @Test
  public void testProcessorLifecycleListener() {
    ProcessorLifecycleListenerFactory mockFactory = mock(ProcessorLifecycleListenerFactory.class);
    TaskApplication testApp = appDesc -> {
      appDesc.withProcessorLifecycleListenerFactory(mockFactory);
    };
    TaskApplicationDescriptorImpl appDesc = new TaskApplicationDescriptorImpl(testApp, config);
    assertEquals(appDesc.getProcessorLifecycleListenerFactory(), mockFactory);
  }
}