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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestSystemAdmin {
  private static final String SYSTEM = "system";
  private static final String STREAM = "stream";
  private static final String OTHER_STREAM = "otherStream";

  /**
   * Given some SSPs, getSSPMetadata should delegate to getSystemStreamMetadata and properly extract the results for the
   * requested SSPs.
   */
  @Test
  public void testGetSSPMetadata() {
    SystemStreamPartition streamPartition0 = new SystemStreamPartition(SYSTEM, STREAM, new Partition(0));
    SystemStreamPartition streamPartition1 = new SystemStreamPartition(SYSTEM, STREAM, new Partition(1));
    SystemStreamPartition otherStreamPartition0 = new SystemStreamPartition(SYSTEM, OTHER_STREAM, new Partition(0));
    SystemAdmin systemAdmin = mock(MySystemAdmin.class);
    SystemStreamMetadata.SystemStreamPartitionMetadata streamPartition0Metadata =
        new SystemStreamMetadata.SystemStreamPartitionMetadata("1", "2", "3");
    SystemStreamMetadata.SystemStreamPartitionMetadata streamPartition1Metadata =
        new SystemStreamMetadata.SystemStreamPartitionMetadata("11", "12", "13");
    SystemStreamMetadata.SystemStreamPartitionMetadata otherStreamPartition0Metadata =
        new SystemStreamMetadata.SystemStreamPartitionMetadata("21", "22", "23");
    when(systemAdmin.getSystemStreamMetadata(ImmutableSet.of(STREAM, OTHER_STREAM))).thenReturn(ImmutableMap.of(
        STREAM, new SystemStreamMetadata(STREAM, ImmutableMap.of(
            new Partition(0), streamPartition0Metadata,
            new Partition(1), streamPartition1Metadata)),
        OTHER_STREAM, new SystemStreamMetadata(OTHER_STREAM, ImmutableMap.of(
            new Partition(0), otherStreamPartition0Metadata))));
    Set<SystemStreamPartition> ssps = ImmutableSet.of(streamPartition0, streamPartition1, otherStreamPartition0);
    when(systemAdmin.getSSPMetadata(ssps)).thenCallRealMethod();
    Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> expected = ImmutableMap.of(
        streamPartition0, streamPartition0Metadata,
        streamPartition1, streamPartition1Metadata,
        otherStreamPartition0, otherStreamPartition0Metadata);
    assertEquals(expected, systemAdmin.getSSPMetadata(ssps));
    verify(systemAdmin).getSystemStreamMetadata(ImmutableSet.of(STREAM, OTHER_STREAM));
  }

  /**
   * Given some SSPs, but missing metadata for one of the streams, getSSPMetadata should delegate to
   * getSystemStreamMetadata and only fill in results for the SSPs corresponding to streams with metadata.
   */
  @Test
  public void testGetSSPMetadataMissingStream() {
    SystemStreamPartition streamPartition0 = new SystemStreamPartition(SYSTEM, STREAM, new Partition(0));
    SystemStreamPartition otherStreamPartition0 = new SystemStreamPartition(SYSTEM, OTHER_STREAM, new Partition(0));
    SystemAdmin systemAdmin = mock(MySystemAdmin.class);
    SystemStreamMetadata.SystemStreamPartitionMetadata streamPartition0Metadata =
        new SystemStreamMetadata.SystemStreamPartitionMetadata("1", "2", "3");
    when(systemAdmin.getSystemStreamMetadata(ImmutableSet.of(STREAM, OTHER_STREAM))).thenReturn(ImmutableMap.of(
        STREAM, new SystemStreamMetadata(STREAM, ImmutableMap.of(new Partition(0), streamPartition0Metadata))));
    Set<SystemStreamPartition> ssps = ImmutableSet.of(streamPartition0, otherStreamPartition0);
    when(systemAdmin.getSSPMetadata(ssps)).thenCallRealMethod();
    Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> expected =
        ImmutableMap.of(streamPartition0, streamPartition0Metadata);
    assertEquals(expected, systemAdmin.getSSPMetadata(ssps));
    verify(systemAdmin).getSystemStreamMetadata(ImmutableSet.of(STREAM, OTHER_STREAM));
  }

  /**
   * Given some SSPs, but missing metadata for one of the SSPs, getSSPMetadata should delegate to
   * getSystemStreamMetadata and only fill in results for the SSPs that have metadata.
   */
  @Test
  public void testGetSSPMetadataMissingPartition() {
    SystemStreamPartition streamPartition0 = new SystemStreamPartition(SYSTEM, STREAM, new Partition(0));
    SystemStreamPartition streamPartition1 = new SystemStreamPartition(SYSTEM, STREAM, new Partition(1));
    SystemAdmin systemAdmin = mock(MySystemAdmin.class);
    SystemStreamMetadata.SystemStreamPartitionMetadata streamPartition0Metadata =
        new SystemStreamMetadata.SystemStreamPartitionMetadata("1", "2", "3");
    when(systemAdmin.getSystemStreamMetadata(ImmutableSet.of(STREAM))).thenReturn(ImmutableMap.of(
        STREAM, new SystemStreamMetadata(STREAM, ImmutableMap.of(new Partition(0), streamPartition0Metadata))));
    Set<SystemStreamPartition> ssps = ImmutableSet.of(streamPartition0, streamPartition1);
    when(systemAdmin.getSSPMetadata(ssps)).thenCallRealMethod();
    Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> expected =
        ImmutableMap.of(streamPartition0, streamPartition0Metadata);
    assertEquals(expected, systemAdmin.getSSPMetadata(ssps));
    verify(systemAdmin).getSystemStreamMetadata(ImmutableSet.of(STREAM));
  }

  /**
   * Looks like Mockito 1.x does not support using thenCallRealMethod with default methods for interfaces, but it works
   * to use this placeholder abstract class.
   */
  private abstract class MySystemAdmin implements ExtendedSystemAdmin { }
}