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

package org.apache.samza.system;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.samza.Partition;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestBoundedSSPIterator {
  private static final SystemStreamPartition SSP = new SystemStreamPartition("test", "test", new Partition(0));

  @Test
  public void testHasNextFalseWhenEnvelopeOutOfBounds() throws InterruptedException {
    SystemConsumer mockConsumer = mock(SystemConsumer.class);
    SystemAdmin mockAdmin = buildMockSystemAdmin();

    int numMessages = 10;
    long endOffset = 5;

    OngoingStubbing<Map<SystemStreamPartition, List<IncomingMessageEnvelope>>> stubbing =
        when(mockConsumer.poll(any(), anyLong()));
    for (int i = 0; i < numMessages; i++) {
      IncomingMessageEnvelope ime = new IncomingMessageEnvelope(SSP, String.valueOf(i), null, i);
      stubbing = stubbing.thenReturn(ImmutableMap.of(SSP, ImmutableList.of(ime)));
    }
    stubbing.thenReturn(ImmutableMap.of(SSP, ImmutableList.of()));

    BoundedSSPIterator iter = new BoundedSSPIterator(mockConsumer, SSP, String.valueOf(endOffset), mockAdmin);

    int consumed = 0;
    while (iter.hasNext()) {
      iter.next();
      consumed++;
    }
    Assert.assertEquals(consumed, endOffset + 1);

    try {
      iter.next();
      Assert.fail("Iterator next call should have failed due to bound check");
    } catch (NoSuchElementException e) {
    }
  }

  @Test
  public void testConsumeAllWithNullBound() throws InterruptedException {
    SystemConsumer mockConsumer = mock(SystemConsumer.class);
    SystemAdmin mockAdmin = buildMockSystemAdmin();

    int numMessages = 10;
    String endOffset = null;

    OngoingStubbing<Map<SystemStreamPartition, List<IncomingMessageEnvelope>>> stubbing =
        when(mockConsumer.poll(any(), anyLong()));
    for (int i = 0; i < numMessages; i++) {
      IncomingMessageEnvelope ime = new IncomingMessageEnvelope(SSP, String.valueOf(i), null, i);
      stubbing = stubbing.thenReturn(ImmutableMap.of(SSP, ImmutableList.of(ime)));
    }
    stubbing.thenReturn(ImmutableMap.of(SSP, ImmutableList.of()));

    BoundedSSPIterator iter = new BoundedSSPIterator(mockConsumer, SSP, endOffset, mockAdmin);

    int consumed = 0;
    while (iter.hasNext()) {
      iter.next();
      consumed++;
    }

    Assert.assertEquals(consumed, numMessages);

    Assert.assertFalse(iter.hasNext());
    try {
      iter.next();
      Assert.fail("Iterator next call should have failed due to bound check");
    } catch (NoSuchElementException e) {
    }
  }

  private SystemAdmin buildMockSystemAdmin() {
    SystemAdmin mockAdmin = mock(SystemAdmin.class);
    when(mockAdmin.offsetComparator(any(), any())).thenAnswer(invocation -> {
        String offset1 = invocation.getArgumentAt(0, String.class);
        String offset2 = invocation.getArgumentAt(1, String.class);

        if (offset1 == null || offset2 == null) {
          return -1;
        }

        return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
      });
    return mockAdmin;
  }
}
