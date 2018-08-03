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
import org.apache.samza.util.CommandLine;


/**
 * The class defines the basic command line arguments for Samza command line scripts.
 */
public class ApplicationRunnerCommandLine extends CommandLine {
  public OptionSpec operationOpt = parser().accepts("operation", "The operation to perform; start, status, kill.")
      .withRequiredArg()
      .ofType(String.class)
      .describedAs("operation=start")
      .defaultsTo("start");

  public ApplicationRunnerOperation getOperation(OptionSet options) {
    String rawOp = options.valueOf(operationOpt).toString();
    return ApplicationRunnerOperation.fromString(rawOp);
  }
}
