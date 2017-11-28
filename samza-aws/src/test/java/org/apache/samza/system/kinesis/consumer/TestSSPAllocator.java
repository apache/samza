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

package org.apache.samza.system.kinesis.consumer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.samza.Partition;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.junit.Test;


public class TestSSPAllocator {
  @Test
  public void testAllocateAndFree() throws NoAvailablePartitionException, NoSuchFieldException, IllegalAccessException {
    int numPartitions = 2;
    String system = "kinesis";
    String stream = "stream";
    List<SystemStreamPartition> ssps = new ArrayList<>();
    IntStream.range(0, numPartitions)
        .forEach(i -> ssps.add(new SystemStreamPartition(system, stream, new Partition(i))));

    SSPAllocator allocator = new SSPAllocator(system);
    ssps.forEach(allocator::free);

    Assert.assertTrue(isSspAvailable(allocator, ssps.get(0)));
    Assert.assertTrue(isSspAvailable(allocator, ssps.get(1)));

    SystemStreamPartition ssp = allocator.allocate(stream);
    Assert.assertFalse(isSspAvailable(allocator, ssps.get(0)));
    Assert.assertTrue(isSspAvailable(allocator, ssps.get(1)));
    Assert.assertEquals(ssp, ssps.get(0));

    ssp = allocator.allocate(stream);
    Assert.assertFalse(isSspAvailable(allocator, ssps.get(0)));
    Assert.assertFalse(isSspAvailable(allocator, ssps.get(1)));
    Assert.assertEquals(ssp, ssps.get(1));

    allocator.free(ssps.get(1));
    Assert.assertFalse(isSspAvailable(allocator, ssps.get(0)));
    Assert.assertTrue(isSspAvailable(allocator, ssps.get(1)));

    allocator.free(ssps.get(0));
    Assert.assertTrue(isSspAvailable(allocator, ssps.get(0)));
    Assert.assertTrue(isSspAvailable(allocator, ssps.get(1)));
  }

  @Test (expected = NoAvailablePartitionException.class)
  public void testAssignMoreThanMaxPartitions() throws NoAvailablePartitionException {
    int numPartitions = 2;
    String system = "kinesis";
    String stream = "stream";
    List<SystemStreamPartition> ssps = new ArrayList<>();
    IntStream.range(0, numPartitions)
        .forEach(i -> ssps.add(new SystemStreamPartition(system, stream, new Partition(i))));

    SSPAllocator allocator = new SSPAllocator(system);
    ssps.forEach(allocator::free);

    allocator.allocate(stream);
    allocator.allocate(stream);
    allocator.allocate(stream); // An exception should be thrown at this point.
  }

  @Test (expected = IllegalArgumentException.class)
  public void testFreeSameSspTwice() throws NoAvailablePartitionException {
    int numPartitions = 2;
    String system = "kinesis";
    String stream = "stream";
    List<SystemStreamPartition> ssps = new ArrayList<>();
    IntStream.range(0, numPartitions)
        .forEach(i -> ssps.add(new SystemStreamPartition(system, stream, new Partition(i))));

    SSPAllocator allocator = new SSPAllocator(system);
    ssps.forEach(allocator::free);

    SystemStreamPartition ssp = allocator.allocate(stream);
    allocator.free(ssp);
    allocator.free(ssp); // An exception should be thrown at this point.
  }

  @Test (expected = IllegalArgumentException.class)
  public void testFreeUnallocatedSsp() throws NoAvailablePartitionException {
    int numPartitions = 2;
    String system = "kinesis";
    String stream = "stream";
    List<SystemStreamPartition> ssps = new ArrayList<>();
    IntStream.range(0, numPartitions)
        .forEach(i -> ssps.add(new SystemStreamPartition(system, stream, new Partition(i))));

    SSPAllocator allocator = new SSPAllocator(system);
    ssps.forEach(allocator::free);

    allocator.allocate(stream);
    allocator.free(ssps.get(1)); // An exception should be thrown at this point.
  }

  @SuppressWarnings("unchecked")
  private boolean isSspAvailable(SSPAllocator sspAllocator, SystemStreamPartition ssp) throws NoSuchFieldException, IllegalAccessException {
    Field f = sspAllocator.getClass().getDeclaredField("availableSsps");
    f.setAccessible(true);
    Map<String, Set<SystemStreamPartition>> availableSsps = (Map<String, Set<SystemStreamPartition>>) f.get(
        sspAllocator);
    return availableSsps.containsKey(ssp.getStream()) && availableSsps.get(ssp.getStream()).contains(ssp);
  }
}
