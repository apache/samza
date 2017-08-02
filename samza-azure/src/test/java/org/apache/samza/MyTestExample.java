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

package org.apache.samza;

import joptsimple.OptionSet;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.util.CommandLine;


public class MyTestExample implements StreamApplication {

  @Override
  public void init(StreamGraph graph, Config config) {
    // Inputs
    MessageStream<Integer> job = graph.getInputStream("myTest", (k, v) -> (Integer) v);

    job.sink(new SinkFunction<Integer>() {
      @Override
      public void apply(Integer message, MessageCollector messageCollector, TaskCoordinator taskCoordinator) {
        System.out.println(message);
        if (message.equals(10)) {
          taskCoordinator.shutdown(TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER);
        }
      }
    });

  }

  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    OptionSet options = cmdLine.parser().parse(args);
    Config config = cmdLine.loadConfig(options);
    LocalApplicationRunner runner = new LocalApplicationRunner(config);
    MyTestExample app = new MyTestExample();

    runner.run(app);
    runner.waitForFinish();

  }

}