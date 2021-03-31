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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.samza.SamzaException;
import org.apache.samza.context.Context;
import org.apache.samza.context.JobContext;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.operators.impl.OperatorImplGraph;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.Clock;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;

public class TestStreamOperatorTask {
  public static OperatorImplGraph getOperatorImplGraph(StreamOperatorTask task) {
    return task.getOperatorImplGraph();
  }

  @Test
  public void testCloseDuringInitializationErrors() throws Exception {
    Context context = mock(Context.class);
    JobContext jobContext = mock(JobContext.class);
    when(context.getJobContext()).thenReturn(jobContext);
    doThrow(new RuntimeException("Failed to get config")).when(jobContext).getConfig();
    StreamOperatorTask operatorTask = new StreamOperatorTask(mock(OperatorSpecGraph.class), mock(Clock.class));
    try {
      operatorTask.init(context);
    } catch (RuntimeException e) {
      if (e instanceof NullPointerException) {
        fail("Unexpected null pointer exception");
      }
    }
    operatorTask.close();
  }

  /**
   * Pass an invalid IME to processAsync. Any exceptions in processAsync should still get propagated through the
   * task callback.
   */
  @Test
  public void testExceptionsInProcessInvokesTaskCallback() throws InterruptedException {
    ExecutorService taskThreadPool = Executors.newFixedThreadPool(2);
    TaskCallback mockTaskCallback = mock(TaskCallback.class);
    MessageCollector mockMessageCollector = mock(MessageCollector.class);
    TaskCoordinator mockTaskCoordinator = mock(TaskCoordinator.class);
    StreamOperatorTask operatorTask = new StreamOperatorTask(mock(OperatorSpecGraph.class));
    operatorTask.setTaskThreadPool(taskThreadPool);

    CountDownLatch failureLatch = new CountDownLatch(1);

    doAnswer(ctx -> {
      failureLatch.countDown();
      return null;
    }).when(mockTaskCallback).failure(anyObject());

    operatorTask.processAsync(mock(IncomingMessageEnvelope.class), mockMessageCollector,
        mockTaskCoordinator, mockTaskCallback);
    failureLatch.await();
  }

  /**
   * Tests if the appropriate SamzaException is propagated to the TaskCallback if there is no InputOperator for a given
   * SystemStream in the OperatorGraph.
   * */
  @Test
  public void testExceptionIfInputOperatorMissing() throws NoSuchFieldException, IllegalAccessException {
    IncomingMessageEnvelope mockIme = mock(IncomingMessageEnvelope.class, RETURNS_DEEP_STUBS);
    SystemStream testSystemStream = new SystemStream("foo", "bar");
    when(mockIme.getSystemStreamPartition().getSystemStream()).thenReturn(testSystemStream);

    OperatorImplGraph mockOperatorImplGraph = mock(OperatorImplGraph.class);
    when(mockOperatorImplGraph.getInputOperator(anyObject())).thenReturn(null);
    StreamOperatorTask operatorTask = new StreamOperatorTask(mock(OperatorSpecGraph.class));
    operatorTask.setOperatorImplGraph(mockOperatorImplGraph);
    TaskCallback mockTaskCallback = mock(TaskCallback.class);
    operatorTask.processAsync(mockIme, mock(MessageCollector.class), mock(TaskCoordinator.class), mockTaskCallback);

    ArgumentCaptor<Throwable> throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(mockTaskCallback, only()).failure(throwableCaptor.capture());
    assertEquals(throwableCaptor.getValue().getClass(), SamzaException.class);
    String expectedErrMessage = String.format("InputOperator not found in OperatorGraph for %s. The available input"
        + " operators are: %s. Please check SystemStream configuration for the `SystemConsumer` and/or task.inputs"
        + " task configuration.", testSystemStream, mockOperatorImplGraph.getAllInputOperators());
    assertEquals(throwableCaptor.getValue().getMessage(), expectedErrMessage);
  }
}
