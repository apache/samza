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
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.task.TaskFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


/**
 * Unit test for {@link TaskAppSpecImpl}
 */
public class TestTaskAppSpecImpl extends AppSpecImplTestBase {
  @Test
  public void testConstructor() {
    TaskApplication mockApp = mock(TaskApplication.class);
    Config mockConf = mock(Config.class);
    TaskAppSpecImpl appSpec = new TaskAppSpecImpl(mockApp, mockConf);
    verify(mockApp, times(1)).describe(appSpec);
    assertEquals(mockConf, appSpec.config);
  }

  @Test
  public void testAddInputStreams() {
    List<String> testInputs = new ArrayList<String>() { { this.add("myinput1"); this.add("myinput2"); } };
    TaskApplication testApp = appSpec -> appSpec.addInputStreams(testInputs);
    Config mockConf = mock(Config.class);
    TaskAppSpecImpl appSpec = new TaskAppSpecImpl(testApp, mockConf);
    assertEquals(appSpec.getInputStreams(), testInputs);
  }

  @Test
  public void testAddOutputStreams() {
    List<String> testOutputs = new ArrayList<String>() { { this.add("myoutput1"); this.add("myoutput2"); } };
    TaskApplication testApp = appSpec -> appSpec.addOutputStreams(testOutputs);
    Config mockConf = mock(Config.class);
    TaskAppSpecImpl appSpec = new TaskAppSpecImpl(testApp, mockConf);
    assertEquals(appSpec.getOutputStreams(), testOutputs);
  }

  @Test
  public void testAddTables() {
    List<TableDescriptor> testTables = new ArrayList<TableDescriptor>() { { this.add(mock(TableDescriptor.class)); } };
    TaskApplication testApp = appSpec -> appSpec.addTables(testTables);
    Config mockConf = mock(Config.class);
    TaskAppSpecImpl appSpec = new TaskAppSpecImpl(testApp, mockConf);
    assertEquals(appSpec.getTables(), testTables);
  }

  @Test
  public void testSetTaskFactory() {
    TaskFactory mockTf = mock(TaskFactory.class);
    TaskApplication testApp = appSpec -> appSpec.setTaskFactory(mockTf);
    Config mockConf = mock(Config.class);
    TaskAppSpecImpl appSpec = new TaskAppSpecImpl(testApp, mockConf);
    assertEquals(appSpec.getTaskFactory(), mockTf);
  }
}
