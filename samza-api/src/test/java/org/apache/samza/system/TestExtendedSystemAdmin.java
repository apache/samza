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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestExtendedSystemAdmin {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  /**
   * Given that an ExtendedSystemAdmin does not override getNewestOffsets, it should default to individually making
   * calls to getNewestOffset.
   */
  @Test
  public void testDefaultGetNewestOffsets() {
    final int numRetries = 3;
    final MyExtendedSystemAdmin extendedSystemAdmin = mock(MyExtendedSystemAdmin.class);
    final SystemStreamPartition ssp = new SystemStreamPartition("system", "stream", new Partition(0));
    final SystemStreamPartition otherSSP = new SystemStreamPartition("otherSystem", "otherStream", new Partition(0));
    final SystemStreamPartition sspNoNewestOffset = new SystemStreamPartition("system", "stream", new Partition(1));
    when(extendedSystemAdmin.getNewestOffset(ssp, numRetries)).thenReturn("0");
    when(extendedSystemAdmin.getNewestOffset(otherSSP, numRetries)).thenReturn("1");
    // check that there is no map entry if the newest offset is missing
    when(extendedSystemAdmin.getNewestOffset(sspNoNewestOffset, numRetries)).thenReturn(null);
    final Set<SystemStreamPartition> allInputSSPs = new HashSet<>(Arrays.asList(ssp, otherSSP, sspNoNewestOffset));
    when(extendedSystemAdmin.getNewestOffsets(allInputSSPs, numRetries)).thenCallRealMethod();

    final Map<SystemStreamPartition, String> expected = new HashMap<>();
    expected.put(ssp, "0");
    expected.put(otherSSP, "1");
    assertEquals(expected, extendedSystemAdmin.getNewestOffsets(allInputSSPs, numRetries));
    verify(extendedSystemAdmin).getNewestOffset(ssp, numRetries);
    verify(extendedSystemAdmin).getNewestOffset(otherSSP, numRetries);
    verify(extendedSystemAdmin).getNewestOffset(sspNoNewestOffset, numRetries);
  }

  /**
   * Given that an ExtendedSystemAdmin does not override getNewestOffsets, it should propagate an exception if an
   * individual call to getNewestOffset throws an exception.
   */
  @Test
  public void testDefaultGetNewestOffsetsException() {
    expectedException.expect(SamzaException.class);
    expectedException.expectMessage("newest offset error");

    final int numRetries = 3;
    final MyExtendedSystemAdmin extendedSystemAdmin = mock(MyExtendedSystemAdmin.class);
    final SystemStreamPartition ssp = new SystemStreamPartition("system", "stream", new Partition(0));
    final SystemStreamPartition otherSSP = new SystemStreamPartition("otherSystem", "otherStream", new Partition(0));
    when(extendedSystemAdmin.getNewestOffset(ssp, numRetries)).thenReturn("0");
    when(extendedSystemAdmin.getNewestOffset(otherSSP, numRetries)).thenThrow(
        new SamzaException("newest offset error"));
    final Set<SystemStreamPartition> allInputSSPs = new HashSet<>(Arrays.asList(ssp, otherSSP));
    when(extendedSystemAdmin.getNewestOffsets(allInputSSPs, numRetries)).thenCallRealMethod();

    extendedSystemAdmin.getNewestOffsets(allInputSSPs, numRetries);
  }

  /**
   * Looks like Mockito 1.x does not support using thenCallRealMethod with default methods for interfaces, but it works
   * to use this placeholder abstract class.
   */
  private abstract class MyExtendedSystemAdmin implements ExtendedSystemAdmin {
  }
}
