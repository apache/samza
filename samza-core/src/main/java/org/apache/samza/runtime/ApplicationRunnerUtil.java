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

package org.apache.samza.runtime;

import org.apache.samza.application.ApplicationUtil;
import org.apache.samza.config.Config;
import org.apache.samza.util.Util;


/**
 * Util class to create {@link ApplicationRunner} from the configuration and run the application
 */
public class ApplicationRunnerUtil {

  /**
   * This method rewrites the passed in config, creates the {@link ApplicationRunner} from the rewritten config and
   * invokes the appropriate operation on the runner based on the specified {@link ApplicationRunnerOperation}.
   * It returns the runner so that the caller could get the status of application on STATUS op.
   *
   * @param originalConfig the original configuration of the application
   * @param op the {@link ApplicationRunnerOperation} that needs to be performed on the Application.
   * @return the {@link ApplicationRunner} object.
   */
  public static ApplicationRunner invoke(Config originalConfig, ApplicationRunnerOperation op,
      ClassLoader classLoader) {
    Config config = Util.rewriteConfig(originalConfig, classLoader);

    ApplicationRunner appRunner =
        ApplicationRunners.getApplicationRunner(ApplicationUtil.fromConfig(config), config);

    switch (op) {
      case RUN:
        appRunner.run(null);
        break;
      case KILL:
        appRunner.kill();
        break;
      case STATUS:
        System.out.println(appRunner.status());
        break;
      default:
        throw new IllegalArgumentException("Unrecognized operation: " + op);
    }
    return appRunner;
  }
}
