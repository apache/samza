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

package org.apache.samza.job.yarn

import org.apache.hadoop.yarn.api.records.Container
import org.joda.time.Period
import org.joda.time.format.{ DateTimeFormatter, ISODateTimeFormat, ISOPeriodFormat, PeriodFormatter }

object YarnContainerUtils {
  val dateFormater = ISODateTimeFormat.dateTime
  val periodFormater = ISOPeriodFormat.standard
}

/**
 * YARN container information plus start time and up time
 */
class YarnContainer(container: Container) {
  val id = container.getId()
  val nodeId = container.getNodeId();
  val nodeHttpAddress = container.getNodeHttpAddress();
  val resource = container.getResource();
  val priority = container.getPriority();
  val containerToken = container.getContainerToken();
  val startTime = System.currentTimeMillis()
  def startTimeStr(dtFormatter: Option[DateTimeFormatter] = None) =
    dtFormatter.getOrElse(YarnContainerUtils.dateFormater).print(startTime)
  val upTime = System.currentTimeMillis()
  def upTimeStr(periodFormatter: Option[PeriodFormatter] = None) =
    periodFormatter.getOrElse(YarnContainerUtils.periodFormater).print(new Period(startTime, upTime))
}
