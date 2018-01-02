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

package org.apache.samza.application;

import java.lang.reflect.Method;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.samza.config.Config;
import org.apache.samza.job.JobRunner$;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.util.CommandLine;
import org.apache.samza.util.Util;


/**
 * This class contains the main() method used by run-app.sh.
 * For a StreamApplication, it creates the {@link ApplicationRunner} based on the config, and then run the application.
 * For a Samza job using low level task API, it will create the JobRunner to run it.
 */
public class ManagedApplicationMain {

  public static class ApplicationMainCommandLine extends CommandLine {
    public OptionSpec operationOpt =
        parser().accepts("operation", "The operation to perform; run, status, kill.")
            .withRequiredArg()
            .ofType(String.class)
            .describedAs("operation=run")
            .defaultsTo("run");

    public ApplicationMainOperation getOperation(OptionSet options) {
      String rawOp = options.valueOf(operationOpt).toString();
      return ApplicationMainOperation.fromString(rawOp);
    }
  }

  public static void main(String[] args) throws Exception {
    ApplicationMainCommandLine cmdLine = new ApplicationMainCommandLine();
    OptionSet options = cmdLine.parser().parse(args);
    Config orgConfig = cmdLine.loadConfig(options);
    Config config = Util.rewriteConfig(orgConfig);
    ApplicationMainOperation op = cmdLine.getOperation(options);
    StreamApplication.AppConfig appConfig = new StreamApplication.AppConfig(config);

    if (appConfig.getAppClass() != null && !appConfig.getAppClass().isEmpty()) {
      StreamApplication app = StreamApplications.createStreamApp(config);
      if (Class.forName(appConfig.getAppClass()).isAssignableFrom(UserDefinedStreamApplication.class)) {
        UserDefinedStreamApplication userApp = Util.getObj(appConfig.getAppClass());
        userApp.init(config, app);
        runCmd(app, op);
      } else {
        Class<?> cls = Class.forName(appConfig.getAppClass());
        Method mainMethod = cls.getMethod("main", String[].class);
        mainMethod.invoke(null, (Object) args);
      }
    } else {
      JobRunner$.MODULE$.main(args);
    }
  }

  public static void runCmd(StreamApplication app, ApplicationMainOperation op) {
    switch (op) {
      case RUN:
        app.run();
        break;
      case KILL:
        app.kill();
        break;
      case STATUS:
        System.out.println(app.status());
        break;
      default:
        throw new IllegalArgumentException("Unrecognized operation: " + op);
    }
  }
}

