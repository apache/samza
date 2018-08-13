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
package org.apache.samza.example;

import java.util.Collections;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.TaskAppDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunners;
import org.apache.samza.storage.kv.RocksDbTableDescriptor;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.util.CommandLine;


/**
 * Test example of a low-level API application (i.e. {@link TaskApplication})
 */
public class TaskApplicationExample implements TaskApplication {

  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ApplicationRunner runner = ApplicationRunners.getApplicationRunner(new TaskApplicationExample(), config);
    runner.run();
    runner.waitForFinish();
  }

  @Override
  public void describe(TaskAppDescriptor appDesc) {
    // add input and output streams
    appDesc.addInputStreams(Collections.singletonList("myinput"));
    appDesc.addOutputStreams(Collections.singletonList("myoutput"));
    TableDescriptor td = new RocksDbTableDescriptor("mytable");
    appDesc.addTables(Collections.singletonList(td));
    // create the task factory based on configuration
    appDesc.setTaskFactory(TaskFactoryUtil.createTaskFactory(appDesc.getConfig()));
  }

}
