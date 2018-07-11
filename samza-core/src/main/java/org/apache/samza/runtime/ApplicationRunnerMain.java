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

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.samza.application.ApplicationRunnable;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplications;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.internal.ApplicationRunner;
import org.apache.samza.task.TaskFactory;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.util.CommandLine;
import org.apache.samza.util.Util;


/**
 * This class contains the main() method used by run-app.sh.
 * For a StreamApplication, it creates the {@link ApplicationRunner} based on the config, and then run the application.
 * For a Samza job using low level task API, it will create the JobRunner to run it.
 */
public class ApplicationRunnerMain {
  // TODO: have the app configs consolidated in one place
  public static final String STREAM_APPLICATION_CLASS_CONFIG = "app.class";

  public static class ApplicationRunnerCommandLine extends CommandLine {
    public OptionSpec operationOpt =
        parser().accepts("operation", "The operation to perform; run, status, kill.")
            .withRequiredArg()
            .ofType(String.class)
            .describedAs("operation=run")
            .defaultsTo("run");

    public ApplicationRunnerOperation getOperation(OptionSet options) {
      String rawOp = options.valueOf(operationOpt).toString();
      return ApplicationRunnerOperation.fromString(rawOp);
    }
  }

  public static void main(String[] args) throws Exception {
    ApplicationRunnerCommandLine cmdLine = new ApplicationRunnerCommandLine();
    OptionSet options = cmdLine.parser().parse(args);
    Config orgConfig = cmdLine.loadConfig(options);
    Config config = Util.rewriteConfig(orgConfig);
    ApplicationRunnerOperation op = cmdLine.getOperation(options);

    ApplicationRunnable appRunnable = config.containsKey(STREAM_APPLICATION_CLASS_CONFIG) ?
        StreamApplications.createRunnable((StreamApplication) Class.forName(config.get(STREAM_APPLICATION_CLASS_CONFIG)).newInstance(), config) :
        StreamApplications.createRunnable((TaskFactory) TaskFactoryUtil.createTaskFactory(config), config);

    switch (op) {
      case RUN:
        appRunnable.run();
        break;
      case KILL:
        appRunnable.kill();
        break;
      case STATUS:
        System.out.println(appRunnable.status());
        break;
      default:
        throw new IllegalArgumentException("Unrecognized operation: " + op);
    }
  }
}
