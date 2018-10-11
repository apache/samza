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

import org.apache.samza.context.Context;
import org.apache.samza.context.JobContext;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.operators.impl.OperatorImplGraph;
import org.apache.samza.util.Clock;
import org.junit.Test;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


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
}
