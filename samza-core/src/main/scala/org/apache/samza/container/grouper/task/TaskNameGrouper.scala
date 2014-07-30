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
package org.apache.samza.container.grouper.task

import org.apache.samza.container.TaskNamesToSystemStreamPartitions

/**
 * After the input SystemStreamPartitions have been mapped to their TaskNames by an implementation of
 * {@link org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouper}, we can then map those groupings onto
 * the {@link org.apache.samza.container.SamzaContainer}s on which they will run.  This class takes
 * those groupings-of-SSPs and groups them together on which container each should run on.  A simple
 * implementation could assign each TaskNamesToSystemStreamPartition to a separate container.  More
 * advanced implementations could examine the TaskNamesToSystemStreamPartition to group by them
 * by data locality, anti-affinity, even distribution of expected bandwidth consumption, etc.
 */
trait TaskNameGrouper {
  /**
   * Group TaskNamesToSystemStreamPartitions onto the containers they will share
   *
   * @param taskNames Pre-grouped SSPs
   * @return Mapping of container ID to set if TaskNames it will run
   */
  def groupTaskNames(taskNames: TaskNamesToSystemStreamPartitions): Map[Int, TaskNamesToSystemStreamPartitions]
}
