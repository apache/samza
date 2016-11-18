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

package org.apache.samza.system

import java.util

import org.apache.samza.config.Config
import org.apache.samza.config.JobConfig.Config2Job
import scala.collection.JavaConverters._


class RegexSystemStreamPartitionMatcher extends SystemStreamPartitionMatcher {
  override def filter(systemStreamPartitions: util.Set[SystemStreamPartition], config: Config): util.Set[SystemStreamPartition] = {
    val sspRegex = config.getSSPMatcherConfigRegex
    val sspSetScala = systemStreamPartitions.asScala
    sspSetScala.filter(s => s.partition.getPartitionId.toString.matches(sspRegex)).asJava
  }
}
