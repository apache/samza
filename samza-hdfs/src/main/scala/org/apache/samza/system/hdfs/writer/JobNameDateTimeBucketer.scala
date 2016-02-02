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


import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.samza.config.Config
import org.apache.samza.system.hdfs.HdfsConfig
import org.apache.samza.system.hdfs.HdfsConfig._


/**
 * Configurable path and file name manager for HdfsWriters. Buckets in the format:
 * /BASE_PATH/JOB_NAME/DATE_FORMAT/FILENAME
 *
 * BASE_PATH is configurable in the job properties file, and defaults to: /user/USERNAME/samza-output
 * JOB_NAME is the job name as configured in the properties file/run args
 * DATE_FORMAT is configurable in the job properties file, and defaults to: YEAR_MONTH_DAY-HOUR
 * FILENAME is a unique per-task and per-file name
 */
class JobNameDateTimeBucketer(systemName: String, config: HdfsConfig) extends Bucketer {
  val basePath = config.getBaseOutputDir(systemName)
  val dateFormatter = config.getDatePathFormatter(systemName)
  val fileBase = config.getFileUniqifier(systemName)

  var partIndex: Int = 0
  var currentDateTime = ""

  /**
   * Test each write to see if we need to cut a new output file based on
   * time bucketing, regardless of hitting configured file size limits etc.
   */
  override def shouldChangeBucket: Boolean = {
    currentDateTime != dateFormatter.format(new java.util.Date)
  }

  /**
   * Given the current FileSystem, returns the next full HDFS path + filename to write to.
   */
  override def getNextWritePath(dfs: FileSystem): Path = {
    val dateTime = dateFormatter.format(new java.util.Date)
    val base = new Path(Seq(basePath, dateTime).mkString("/"))
    if (dateTime != currentDateTime) {
      currentDateTime = dateTime
      FileSystem.mkdirs(dfs, base, FsPermission.getDirDefault)
    }
    new Path(base, nextPartFile)
  }

  protected def nextPartFile: String = {
    partIndex += 1
    if (partIndex > 99999) partIndex = 1
    fileBase + "%05d".format(partIndex)
  }

}
