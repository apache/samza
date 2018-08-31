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
package org.apache.samza.application;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.samza.config.Config;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.operators.descriptors.base.stream.InputDescriptor;
import org.apache.samza.operators.descriptors.base.stream.OutputDescriptor;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.runtime.ProcessorLifecycleListenerFactory;
import org.apache.samza.task.TaskFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Unit test for {@link TaskAppDescriptorImpl}
 */
public class TestTaskAppDescriptorImpl {

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
      TableDescriptor mock1 = mock(TableDescriptor.class);
      TableDescriptor mock2 = mock(TableDescriptor.class);
      when(mock1.getTableId()).thenReturn("test-table1");
      when(mock2.getTableId()).thenReturn("test-table2");
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
    TaskAppDescriptorImpl appDesc = new TaskAppDescriptorImpl(mockApp, config);
    verify(mockApp).describe(appDesc);
    assertEquals(config, appDesc.config);
  }

  @Test
  public void testAddInputStreams() {
    TaskApplication testApp = appDesc -> {
      mockInputs.forEach(appDesc::addInputStream);
    };
    TaskAppDescriptorImpl appDesc = new TaskAppDescriptorImpl(testApp, config);
    assertEquals(mockInputs.toArray(), appDesc.getInputDescriptors().values().toArray());
  }

  @Test
  public void testAddOutputStreams() {
    TaskApplication testApp = appDesc -> {
      mockOutputs.forEach(appDesc::addOutputStream);
    };
    TaskAppDescriptorImpl appDesc = new TaskAppDescriptorImpl(testApp, config);
    assertEquals(mockOutputs.toArray(), appDesc.getOutputDescriptors().values().toArray());
  }

  @Test
  public void testAddTables() {
    TaskApplication testApp = appDesc -> {
      mockTables.forEach(appDesc::addTable);
    };
    TaskAppDescriptorImpl appDesc = new TaskAppDescriptorImpl(testApp, config);
    assertEquals(mockTables, appDesc.getTableDescriptors());
  }

  @Test
  public void testSetTaskFactory() {
    TaskFactory mockTf = mock(TaskFactory.class);
    TaskApplication testApp = appDesc -> appDesc.setTaskFactory(mockTf);
    TaskAppDescriptorImpl appDesc = new TaskAppDescriptorImpl(testApp, config);
    assertEquals(appDesc.getTaskFactory(), mockTf);
  }

  @Test
  public void testContextManager() {
    ContextManager cntxMan = mock(ContextManager.class);
    TaskApplication testApp = appDesc -> {
      appDesc.withContextManager(cntxMan);
    };
    TaskAppDescriptorImpl appDesc = new TaskAppDescriptorImpl(testApp, config);
    assertEquals(appDesc.getContextManager(), cntxMan);
  }

  @Test
  public void testProcessorLifecycleListener() {
    ProcessorLifecycleListenerFactory mockFactory = mock(ProcessorLifecycleListenerFactory.class);
    TaskApplication testApp = appDesc -> {
      appDesc.withProcessorLifecycleListenerFactory(mockFactory);
    };
    TaskAppDescriptorImpl appDesc = new TaskAppDescriptorImpl(testApp, config);
    assertEquals(appDesc.getProcessorLifecycleListenerFactory(), mockFactory);
  }

}