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

import java.util.Optional;
import joptsimple.OptionSet;
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.application.ApplicationUtil;
import org.apache.samza.config.Config;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.runtime.ApplicationRunnerMain;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunners;
import org.apache.samza.util.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ApplicationRunnerMain} was designed for deploying {@link SamzaApplication} in yarn
 * and doesn't work for in standalone.
 *
 * This runner class is built for standalone failure tests and not recommended for general use.
 */
public class LocalApplicationRunnerMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalApplicationRunnerMain.class);

  public static void main(String[] args) {
    ApplicationRunnerMain.ApplicationRunnerCommandLine cmdLine = new ApplicationRunnerMain.ApplicationRunnerCommandLine();
    OptionSet options = cmdLine.parser().parse(args);
    Config orgConfig = cmdLine.loadConfig(options);
    Config config = ConfigUtil.rewriteConfig(orgConfig);

    SamzaApplication app = ApplicationUtil.fromConfig(config);
    ApplicationRunner runner = ApplicationRunners.getApplicationRunner(app, config);

    try {
      LOGGER.info("Launching stream application: {} to run.", app);
      runner.run(buildExternalContext(config).orElse(null));
      runner.waitForFinish();
    } catch (Exception e) {
      LOGGER.error("Exception occurred when running application: {}.", app, e);
    }
  }

  private static Optional<ExternalContext> buildExternalContext(@SuppressWarnings("unused") Config config) {
    /*
     * By default, use an empty ExternalContext here. In a custom fork of Samza, this can be implemented to pass
     * a non-empty ExternalContext. Only config should be used to build the external context. In the future, components
     * like the application descriptor may not be available.
     */
    return Optional.empty();
  }
}
