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

import org.apache.samza.SamzaException
import org.apache.samza.config.{JobConfig, Config}
import org.apache.samza.config.JobConfig.Config2Job
import scala.collection.JavaConverters._

class RangeSystemStreamPartitionMatcher extends SystemStreamPartitionMatcher {

  override def filter(systemStreamPartitions: util.Set[SystemStreamPartition], config: Config): util.Set[SystemStreamPartition] = {
    val sspRanges = config.getSSPMatcherConfigRanges
    val ranges: Array[String] = sspRanges.split(",")
    val rangeMap = collection.mutable.Map[Int, Int]()

    // Accept single or multiple partition ranges in one config - 2,3,7-9,1 or 19
    // Overlapping ranges are fine
    val rxS = "(\\d+)".r         // single digits
    val rxR = "(\\d+-\\d+)".r   // range like 7-9

    ranges.foreach({
      case rxS(s) => rangeMap.put(s.toInt, s.toInt)
      case rxR(r) =>
        val s = r.split("-")
        (s(0).toInt to s(1).toInt).foreach(k => rangeMap.put(k, k))
      case _ =>
        val error = "Invalid partition range configuration '%s': %s"
          .format(JobConfig.SSP_MATCHER_CONFIG_RANGES, sspRanges)
        throw new SamzaException(error)
    })
    val sspSetScala = systemStreamPartitions.asScala
    sspSetScala.filter(s => rangeMap.contains(s.partition.getPartitionId)).asJava
  }

}
