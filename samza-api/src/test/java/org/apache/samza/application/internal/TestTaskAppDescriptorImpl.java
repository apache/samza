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
package org.apache.samza.application.internal;

import java.util.ArrayList;
import java.util.List;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.runtime.ProcessorLifecycleListenerFactory;
import org.apache.samza.task.TaskFactory;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit test for {@link TaskAppDescriptorImpl}
 */
public class TestTaskAppDescriptorImpl {

  private Config config = mock(Config.class);

  @Test
  public void testConstructor() {
    TaskApplication mockApp = mock(TaskApplication.class);
    TaskAppDescriptorImpl appDesc = new TaskAppDescriptorImpl(mockApp, config);
    verify(mockApp, times(1)).describe(appDesc);
    assertEquals(config, appDesc.config);
  }

  @Test
  public void testAddInputStreams() {
    List<String> testInputs = new ArrayList<String>() { { this.add("myinput1"); this.add("myinput2"); } };
    TaskApplication testApp = appDesc -> appDesc.addInputStreams(testInputs);
    TaskAppDescriptorImpl appDesc = new TaskAppDescriptorImpl(testApp, config);
    assertEquals(appDesc.getInputStreams(), testInputs);
  }

  @Test
  public void testAddOutputStreams() {
    List<String> testOutputs = new ArrayList<String>() { { this.add("myoutput1"); this.add("myoutput2"); } };
    TaskApplication testApp = appDesc -> appDesc.addOutputStreams(testOutputs);
    TaskAppDescriptorImpl appDesc = new TaskAppDescriptorImpl(testApp, config);
    assertEquals(appDesc.getOutputStreams(), testOutputs);
  }

  @Test
  public void testAddTables() {
    List<TableDescriptor> testTables = new ArrayList<TableDescriptor>() { { this.add(mock(TableDescriptor.class)); } };
    TaskApplication testApp = appDesc -> appDesc.addTables(testTables);
    TaskAppDescriptorImpl appDesc = new TaskAppDescriptorImpl(testApp, config);
    assertEquals(appDesc.getTables(), testTables);
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
