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
package org.apache.samza.task;

import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.application.descriptors.StreamApplicationDescriptorImpl;
import org.apache.samza.application.descriptors.TaskApplicationDescriptorImpl;
import org.apache.samza.config.ConfigException;
import org.apache.samza.operators.OperatorSpecGraph;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Test methods to create {@link StreamTaskFactory} or {@link AsyncStreamTaskFactory} based on task class configuration
 */
public class TestTaskFactoryUtil {

  @Test
  public void testStreamTaskClass() {
    TaskFactory retFactory = TaskFactoryUtil.getTaskFactory(MockStreamTask.class.getName());
    assertTrue(retFactory instanceof StreamTaskFactory);
    assertTrue(((StreamTaskFactory) retFactory).createInstance() instanceof MockStreamTask);

    try {
      TaskFactoryUtil.getTaskFactory("no.such.class");
      fail("Should have failed w/ no.such.class");
    } catch (ConfigException cfe) {
      // expected
    }
  }

  @Test
  public void testAsyncStreamTask() {
    TaskFactory retFactory = TaskFactoryUtil.getTaskFactory(MockAsyncStreamTask.class.getName());
    assertTrue(retFactory instanceof AsyncStreamTaskFactory);
    assertTrue(((AsyncStreamTaskFactory) retFactory).createInstance() instanceof MockAsyncStreamTask);

    try {
      TaskFactoryUtil.getTaskFactory("no.such.class");
      fail("Should have failed w/ no.such.class");
    } catch (ConfigException cfe) {
      // expected
    }
  }

  @Test
  public void testFinalizeTaskFactory() throws NoSuchFieldException, IllegalAccessException {
    TaskFactory mockFactory = mock(TaskFactory.class);
    try {
      TaskFactoryUtil.finalizeTaskFactory(mockFactory, true, 0, 0);
      fail("Should have failed with validation");
    } catch (SamzaException se) {
      // expected
    }
    StreamTaskFactory mockStreamFactory = mock(StreamTaskFactory.class);
    Object retFactory = TaskFactoryUtil.finalizeTaskFactory(mockStreamFactory, true, 0, 0);
    assertEquals(retFactory, mockStreamFactory);

    AsyncStreamTaskFactory mockAsyncStreamFactory = mock(AsyncStreamTaskFactory.class);
    try {
      TaskFactoryUtil.finalizeTaskFactory(mockAsyncStreamFactory, true, 0, 0);
      fail("Should have failed");
    } catch (SamzaException se) {
      // expected
    }

    retFactory = TaskFactoryUtil.finalizeTaskFactory(mockAsyncStreamFactory, false, 0, 0);
    assertEquals(retFactory, mockAsyncStreamFactory);
  }

  // test getTaskFactory with StreamApplicationDescriptor
  @Test
  public void testGetTaskFactoryWithStreamAppDescriptor() {
    StreamApplicationDescriptorImpl mockStreamApp = mock(StreamApplicationDescriptorImpl.class);
    OperatorSpecGraph mockSpecGraph = mock(OperatorSpecGraph.class);
    when(mockStreamApp.getOperatorSpecGraph()).thenReturn(mockSpecGraph);
    TaskFactory streamTaskFactory = TaskFactoryUtil.getTaskFactory(mockStreamApp);
    assertTrue(streamTaskFactory instanceof StreamTaskFactory);
    StreamTask streamTask = ((StreamTaskFactory) streamTaskFactory).createInstance();
    assertTrue(streamTask instanceof StreamOperatorTask);
    verify(mockSpecGraph).clone();
  }

  // test getTaskFactory with TaskApplicationDescriptor
  @Test
  public void testGetTaskFactoryWithTaskAppDescriptor() {
    TaskApplicationDescriptorImpl mockTaskApp = mock(TaskApplicationDescriptorImpl.class);
    TaskFactory mockTaskFactory = mock(TaskFactory.class);
    when(mockTaskApp.getTaskFactory()).thenReturn(mockTaskFactory);
    TaskFactory taskFactory = TaskFactoryUtil.getTaskFactory(mockTaskApp);
    assertEquals(mockTaskFactory, taskFactory);
  }

  // test getTaskFactory with invalid ApplicationDescriptorImpl
  @Test(expected = IllegalArgumentException.class)
  public void testGetTaskFactoryWithInvalidAddDescriptorImpl() {
    ApplicationDescriptorImpl mockInvalidApp = mock(ApplicationDescriptorImpl.class);
    TaskFactoryUtil.getTaskFactory(mockInvalidApp);
  }
}
