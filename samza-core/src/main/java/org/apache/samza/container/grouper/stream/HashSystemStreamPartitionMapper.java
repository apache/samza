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

import com.google.common.base.Preconditions;
import org.apache.samza.Partition;
import org.apache.samza.system.SystemStreamPartition;

/**
 * A SystemStreamPartitionMapper that uses the hash partitioning function of the producer to map a {@link SystemStreamPartition} to
 * correct previous {@link SystemStreamPartition} after the stream expansion.
 */
public class HashSystemStreamPartitionMapper implements SystemStreamPartitionMapper {

  @Override
  public SystemStreamPartition getPreviousSSP(SystemStreamPartition currentSystemStreamPartition, int previousPartitionCount, int afterPartitionCount) {
    Preconditions.checkNotNull(currentSystemStreamPartition);
    Preconditions.checkArgument(afterPartitionCount % previousPartitionCount == 0,
                                String.format("New partition count: %d should be a multiple of previous partition count: %d.", afterPartitionCount, previousPartitionCount));
    Partition partition = currentSystemStreamPartition.getPartition();
    Preconditions.checkNotNull(partition, String.format("SystemStreamPartition: %s cannot have null partition", currentSystemStreamPartition));
    int currentPartitionId = partition.getPartitionId();
    int previousPartitionId = currentPartitionId % previousPartitionCount;
    return new SystemStreamPartition(currentSystemStreamPartition.getSystemStream(), new Partition(previousPartitionId));
  }
}
