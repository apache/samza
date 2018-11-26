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

import org.apache.samza.system.SystemStreamPartition;

/**
 * Input streams of a samza job can be either expanded or contracted by the user.
 * This abstraction determines the previous {@link SystemStreamPartition} for a {@link SystemStreamPartition}
 * of a input stream after the stream expansion. This will be used in {@link SystemStreamPartitionGrouper}
 * implementations to generate partition expansion aware task assignments.
 */
public interface StreamPartitionMapper {

  /**
   * Determines the previous {@link SystemStreamPartition} for a {@link SystemStreamPartition}
   * of a input stream after the stream expansion.
   * @param currentSystemStreamPartition denotes the current partition after the stream expansion.
   * @param partitionCountBeforeExpansion the partition count of the stream before the stream expansion.
   * @param partitionCountAfterExpansion the partition count of the stream after the stream expansion.
   * @return the mapped {@link SystemStreamPartition}.
   */
  SystemStreamPartition getSSPAfterPartitionChange(SystemStreamPartition currentSystemStreamPartition,
                                                   int partitionCountBeforeExpansion, int partitionCountAfterExpansion);
}
