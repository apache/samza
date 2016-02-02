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

package org.apache.samza.webapp

import org.scalatra._
import scalate.ScalateSupport
import org.apache.samza.job.yarn.{SamzaAppState}
import org.apache.samza.config.Config
import scala.collection.JavaConversions._
import scala.collection.immutable.TreeMap
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.webapp.util.WebAppUtils

class ApplicationMasterWebServlet(config: Config, state: SamzaAppState) extends ScalatraServlet with ScalateSupport {
  val yarnConfig = new YarnConfiguration

  before() {
    contentType = "text/html"
  }

  get("/") {
    layoutTemplate("/WEB-INF/views/index.scaml",
      "config" -> TreeMap(config.sanitize.toMap.toArray: _*),
      "state" -> state,
      "rmHttpAddress" -> WebAppUtils.getRMWebAppURLWithScheme(yarnConfig))
  }
}
