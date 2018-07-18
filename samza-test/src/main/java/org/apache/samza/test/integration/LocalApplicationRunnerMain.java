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

package org.apache.samza.test.integration;

import joptsimple.OptionSet;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.ApplicationRunnerMain;
import org.apache.samza.runtime.ApplicationRuntime;
import org.apache.samza.runtime.ApplicationRuntimes;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.runtime.ApplicationRunnerMain.*;

/**
 * {@link ApplicationRunnerMain} was designed for deploying {@link StreamApplication} in yarn
 * and doesn't work for in standalone.
 *
 * This runner class is built for standalone failure tests and not recommended for general use.
 */
public class LocalApplicationRunnerMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalApplicationRunnerMain.class);

  public static void main(String[] args) throws Exception {
    ApplicationRunnerMain.ApplicationRunnerCommandLine cmdLine = new ApplicationRunnerMain.ApplicationRunnerCommandLine();
    OptionSet options = cmdLine.parser().parse(args);
    Config orgConfig = cmdLine.loadConfig(options);
    Config config = Util.rewriteConfig(orgConfig);

    ApplicationRuntime appRuntime = ApplicationRuntimes.createStreamApp((StreamApplication) Class.forName(config.get(STREAM_APPLICATION_CLASS_CONFIG)).newInstance(), config);

    try {
      LOGGER.info("Launching stream application: {} to start.", appRuntime);
      appRuntime.start();
      appRuntime.waitForFinish();
    } catch (Exception e) {
      LOGGER.error("Exception occurred when running application: {}.", appRuntime, e);
    }
  }
}
