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
package org.apache.samza.container.grouper.stream;

import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;

import java.util.Map;
import java.util.Set;

/**
 * Group a set of SystemStreamPartitions into logical taskNames that share a common characteristic, defined
 * by the implementation.  Each taskName has a key that uniquely describes what sets may be in it, but does
 * not generally enumerate the elements of those sets.  For example, a SystemStreamPartitionGrouper that
 * groups SystemStreamPartitions (each with 4 partitions) by their partition, would end up generating
 * four TaskNames: 0, 1, 2, 3.  These TaskNames describe the partitions but do not list all of the
 * SystemStreamPartitions, which allows new SystemStreamPartitions to be added later without changing
 * the definition of the TaskNames, assuming these new SystemStreamPartitions do not have more than
 * four partitions.  On the other hand, a SystemStreamPartitionGrouper that wanted each SystemStreamPartition
 * to be its own, unique group would use the SystemStreamPartition's entire description to generate
 * the TaskNames.
 */
public interface SystemStreamPartitionGrouper {
  public Map<TaskName, Set<SystemStreamPartition>> group(Set<SystemStreamPartition> ssps);
}
