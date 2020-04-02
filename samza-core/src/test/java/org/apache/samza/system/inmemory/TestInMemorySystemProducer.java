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
package org.apache.samza.system.inmemory;

import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;


public class TestInMemorySystemProducer {

  @Mock
  private InMemoryManager inMemoryManager;

  private InMemorySystemProducer inMemorySystemProducer;
  private boolean testFinished;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    this.inMemorySystemProducer = new InMemorySystemProducer("systemName", this.inMemoryManager);
    this.testFinished = false;
  }

  /**
   * Test keys of type byte[] goes to the same partition if they have the same contents.
   */
  @Test
  public void testPartition() {
    doReturn(1000).when(inMemoryManager).getPartitionCountForSystemStream(any());
    doAnswer(new Answer<Void>() {
      int partitionOfFirstMessage = -1;
      int partitionOfSecondMessage = -2;

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        SystemStreamPartition ssp = invocation.getArgumentAt(0, SystemStreamPartition.class);
        if (partitionOfFirstMessage == -1) {
          partitionOfFirstMessage = ssp.getPartition().getPartitionId();
        } else {
          partitionOfSecondMessage = ssp.getPartition().getPartitionId();
          Assert.assertEquals(partitionOfFirstMessage, partitionOfSecondMessage);
          testFinished = true;
        }
        return null;
      }
    }).when(inMemoryManager).put(any(), any(), any());

    byte[] key1 = new byte[]{1, 2, 3};
    byte[] key2 = new byte[]{1, 2, 3};
    SystemStream systemStream = new SystemStream("TestSystem", "TestStream");
    OutgoingMessageEnvelope outgoingMessageEnvelope1 = new OutgoingMessageEnvelope(systemStream, key1, null);
    OutgoingMessageEnvelope outgoingMessageEnvelope2 = new OutgoingMessageEnvelope(systemStream, key2, null);
    inMemorySystemProducer.send("TestSource", outgoingMessageEnvelope1);
    inMemorySystemProducer.send("TestSource", outgoingMessageEnvelope2);
    Assert.assertTrue(testFinished);
  }
}
