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
package org.apache.samza.container.grouper.stream

import org.apache.samza.container.TaskName
import java.util
import org.apache.samza.system.SystemStreamPartition
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import org.apache.samza.config.Config

/**
 * Group the {@link org.apache.samza.system.SystemStreamPartition}s by their Partition, with the key being
 * the string representation of the Partition.
 */
class GroupByPartition extends SystemStreamPartitionGrouper {
  override def group(ssps: util.Set[SystemStreamPartition]) = {
    ssps.groupBy( s => new TaskName("Partition " + s.getPartition.getPartitionId) )
      .map(r => r._1 -> r._2.asJava)
  }
}

class GroupByPartitionFactory extends SystemStreamPartitionGrouperFactory {
  override def getSystemStreamPartitionGrouper(config: Config): SystemStreamPartitionGrouper = new GroupByPartition
}
