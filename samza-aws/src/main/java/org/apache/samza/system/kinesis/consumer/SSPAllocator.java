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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.Validate;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SSPAllocator is responsible for assigning Samza SystemStreamPartitions (SSPs). It provides two APIs:
 * <ul>
 *   <li> allocate: Given a stream, returns free ssp.
 *   <li> free: Adds ssp back to the free pool.
 * </ul>
 * A free (unallocated) ssp is returned for every allocate request and when there is no available ssp to allocate,
 * the allocator throws NoAvailablePartitionException. Allocator could run out of free ssps as a result of dynamic
 * shard splits.
 */
class SSPAllocator {
  private static final Logger LOG = LoggerFactory.getLogger(SSPAllocator.class.getName());

  private final String system;
  private final Map<String, Set<SystemStreamPartition>> availableSsps = new HashMap<>();

  SSPAllocator(String system) {
    this.system = system;
  }

  synchronized SystemStreamPartition allocate(String stream) throws NoAvailablePartitionException {
    Validate.isTrue(availableSsps.get(stream) != null,
        String.format("availableSsps is null for stream %s", stream));

    if (availableSsps.get(stream).size() <= 0) {
      // Set a flag in system consumer so that it could throw an exception in the subsequent poll.
      throw new NoAvailablePartitionException(String.format("More shards detected for stream %s than initially"
          + " registered. Could be the result of dynamic resharding.", stream));
    }

    SystemStreamPartition ssp = availableSsps.get(stream).iterator().next();
    availableSsps.get(stream).remove(ssp);

    LOG.info("Number of unassigned partitions for system-stream {} is {}.", ssp.getSystemStream(),
        availableSsps.get(ssp.getStream()).size());
    return ssp;
  }

  synchronized void free(SystemStreamPartition ssp) {
    Validate.isTrue(system.equals(ssp.getSystem()), String.format("Assigning ssp %s from different system."
        + " Expected system is %s.", ssp, system));

    boolean success = availableSsps.computeIfAbsent(ssp.getStream(), p -> new HashSet<>()).add(ssp);
    Validate.isTrue(success, String.format("Ssp %s is already in free pool.", ssp));

    LOG.info("Number of unassigned partitions for system-stream {} is {}.", ssp.getSystemStream(),
        availableSsps.get(ssp.getStream()).size());
  }
}
