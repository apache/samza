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

package org.apache.samza.coordinator.stream

import java.util

import org.apache.samza.util.CommandLine
import joptsimple.OptionSet
import org.apache.samza.config.{ApplicationConfig, JobConfig, MapConfig}
import org.apache.samza.job.JobRunner.{info, warn, _}

class CoordinatorStreamWriterCommandLine extends CommandLine {

  val messageType =
    parser.accepts("type", "the type of the message being sent.")
        .withRequiredArg
        .ofType(classOf[java.lang.String])
        .describedAs("Required field. This field is the type of the message being sent." +
        " The possible values are {\"set-config\"}")


  val messageKey =
    parser.accepts("key", "the type of the message being sent")
        .withRequiredArg
        .ofType(classOf[java.lang.String])
        .describedAs("key of the message")

  val messageValue =
    parser.accepts("value", "the type of the message being sent")
        .withRequiredArg
        .ofType(classOf[java.lang.String])
        .describedAs("value of the message")

  def loadType(options: OptionSet) = {
    if (!options.has(messageType)) {
      parser.printHelpOn(System.err)
      System.exit(-1)
    }
    options.valueOf(messageType)
  }

  def loadKey(options: OptionSet): java.lang.String = {
    if (options.has(messageKey)) {
      options.valueOf(messageKey)
    } else {
      null
    }
  }

  def loadValue(options: OptionSet) = {
    var value: java.lang.String = null
    if (options.has(messageValue)) {
      value = options.valueOf(messageValue)
    }

    value
  }

  def genJobConfigs(config: MapConfig): MapConfig = {
    var genConfig = new util.HashMap[String, String](config)

    if (genConfig.containsKey(JobConfig.JOB_ID))
      warn("%s is a deprecated configuration, use %s instead." format(JobConfig.JOB_ID, ApplicationConfig.APP_ID))

    if (genConfig.containsKey(JobConfig.JOB_NAME))
      warn("%s is a deprecated configuration, use %s instead." format(JobConfig.JOB_NAME, ApplicationConfig.APP_NAME))

    if (genConfig.containsKey(ApplicationConfig.APP_NAME)) {
      val appName = genConfig.get(ApplicationConfig.APP_NAME)
      info("app.name is defined, setting job.name equal to app.name value: %s" format(appName))
      genConfig.put(JobConfig.JOB_NAME, appName)
    }

    if (genConfig.containsKey(ApplicationConfig.APP_ID)) {
      val appId = genConfig.get(ApplicationConfig.APP_ID)
      info("app.id is defined, setting job.id equal to app.name value: %s" format(appId))
      genConfig.put(JobConfig.JOB_ID, appId)
    }
    new MapConfig(genConfig)
  }
}
