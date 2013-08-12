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

package org.apache.samza.config

object JobConfig {
  // job config constants
  val STREAM_JOB_FACTORY_CLASS = "job.factory.class" // streaming.job_factory_class
  val CONFIG_REWRITERS = "job.config.rewriters" // CSV list of config rewriter classes to apply
  val CONFIG_REWRITER_CLASS = "job.config.rewriter.%s.class"
  val JOB_NAME = "job.name" // streaming.job_name
  val JOB_ID = "job.id" // streaming.job_id

  implicit def Config2Job(config: Config) = new JobConfig(config)
}

class JobConfig(config: Config) extends ScalaMapConfig(config) {
  def getName = getOption(JobConfig.JOB_NAME)

  def getStreamJobFactoryClass = getOption(JobConfig.STREAM_JOB_FACTORY_CLASS)

  def getJobId = getOption(JobConfig.JOB_ID)

  def getConfigRewriters = getOption(JobConfig.CONFIG_REWRITERS)

  def getConfigRewriterClass(name: String) = getOption(JobConfig.CONFIG_REWRITER_CLASS format name)
}
