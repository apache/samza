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

package org.apache.samza.system.hdfs.writer


import org.apache.samza.system.hdfs.HdfsConfig
import org.apache.samza.config.Config
import org.apache.hadoop.fs.{FileSystem, Path}


object Bucketer {

  /**
   * Factory for the Bucketer subclass the user configured in the job properties file.
   */
  def getInstance(systemName: String, samzaConfig: HdfsConfig): Bucketer = {
    Class.forName(samzaConfig.getHdfsBucketerClassName(systemName))
      .getConstructor(classOf[String], classOf[HdfsConfig])
      .newInstance(systemName, samzaConfig)
      .asInstanceOf[Bucketer]
  }

}


/**
 * Utility for plugging in various methods of bucketing. Used by HdfsWriters
 * when a file is completed and a new file is created. Includes path and filename.
 */
trait Bucketer {

  /**
   * Has an event occured that requires us to change the current
   * HDFS path for output files. Could be time passing in a date bucket, etc.
   */
  def shouldChangeBucket: Boolean

  /**
   * Given the current FileSystem, generate a new HDFS write path and file name.
   */
  def getNextWritePath(dfs: FileSystem): Path

}
