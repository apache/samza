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


import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.samza.job.StreamJobFactory
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.samza.config.Config
import org.apache.samza.util.hadoop.HttpFileSystem
import org.apache.samza.util.Logging

class YarnJobFactory extends StreamJobFactory with Logging {
  def getJob(config: Config) = {
    // TODO fix this. needed to support http package locations.
    val hConfig = new YarnConfiguration
    hConfig.set("fs.http.impl", classOf[HttpFileSystem].getName)
    hConfig.set("fs.https.impl", classOf[HttpFileSystem].getName)
    hConfig.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    // pass along the RM config if has any
    if (config.containsKey(YarnConfiguration.RM_ADDRESS)) {
      hConfig.set(YarnConfiguration.RM_ADDRESS, config.get(YarnConfiguration.RM_ADDRESS, "0.0.0.0:8032"))
    }

    if (config.containsKey("fs.certfs.impl.override")) { // TODO: change to use constants from CertFSConstants once that CertFSConstants.java is in
      hConfig.set("fs.certfs.impl", config.get("fs.certfs.impl.override"))
      logger.info("samza job config fs.certfs.impl.override is used for yarn.")
    }

    new YarnJob(config, hConfig)
  }
}
