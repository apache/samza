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

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.operators.StreamGraph;


/**
 * This interface defines a template for stream application that user will implement to initialize operator DAG in {@link StreamGraph}.
 *
 * <p>
 * User program implements {@link StreamApplication#init(StreamGraph, Config)} method to initialize the transformation logic
 * from all input streams to output streams. A simple user code example is shown below:
 * </p>
 *
 * <pre>{@code
 * public class PageViewCounterExample implements StreamApplication {
 *   Set<String> blackListMembers = new HashSet<>();
 *
 *   public void init(StreamGraph graph, Config config) {
 *     MessageStream<PageViewEvent> pageViewEvents = graph.getInputStream("pageViewEventStream", (k, m) -> (PageViewEvent) m);
 *     OutputStream<String, PageViewEvent, PageViewEvent> pageViewEventFilteredStream = graph
 *       .getOutputStream("pageViewEventFiltered", m -> m.memberId, m -> m);
 *
 *     pageViewEvents
 *       .filter(m -> !this.blackListMembers.contains(m.memberId))
 *       .sendTo(pageViewEventFilteredStream);
 *   }
 *
 *   // local execution mode
 *   public static void main(String[] args) {
 *     CommandLine cmdLine = new CommandLine();
 *     Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
 *     PageViewCounterExample userApp = new PageViewCounterExample();
 *     userApp.initBlackList(config);
 *     ApplicationRunner localRunner = ApplicationRunner.getLocalRunner(config);
 *     localRunner.run(userApp);
 *   }
 *
 *   private void initBlackList(Config config) {
 *     // Read the blacklist from config
 *   }
 * }
 * }</pre>
 *
 */
@InterfaceStability.Unstable
public interface StreamApplication {

  /**
   * Users are required to implement this abstract method to initialize the processing logic of the application, in terms
   * of a DAG of {@link org.apache.samza.operators.MessageStream}s and operators
   *
   * @param graph  an empty {@link StreamGraph} object to be initialized
   * @param config  the {@link Config} of the application
   */
  void init(StreamGraph graph, Config config);

}
