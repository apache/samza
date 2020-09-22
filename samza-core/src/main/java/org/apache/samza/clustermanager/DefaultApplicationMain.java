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
package org.apache.samza.clustermanager;

import com.google.common.annotations.VisibleForTesting;
import joptsimple.OptionSet;
import org.apache.samza.application.ApplicationUtil;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.ApplicationRunnerMain;
import org.apache.samza.util.ConfigUtil;


public class DefaultApplicationMain {

  public static void main(String[] args) {
    run(args);
  }

  @VisibleForTesting
  static void run(String[] args) {
    // This branch is ONLY for Yarn deployments, standalone apps uses offspring
    final ApplicationRunnerMain.ApplicationRunnerCommandLine cmdLine = new ApplicationRunnerMain.ApplicationRunnerCommandLine();
    cmdLine.parser().allowsUnrecognizedOptions();

    final OptionSet options = cmdLine.parser().parse(args);
    // load full job config with ConfigLoader
    final Config originalConfig = ConfigUtil.loadConfig(cmdLine.loadConfig(options));

    JobCoordinatorLaunchUtil.run(ApplicationUtil.fromConfig(originalConfig), originalConfig);
  }
}
