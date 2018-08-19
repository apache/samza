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

/**
 * Describes and initializes the transforms for processing message streams and generating results.
 * <p>
 * The following example removes page views older than 1 hour from the input stream:
 * <pre>{@code
 * public class PageViewCounter implements StreamApplication {
 *   public void describe(StreamAppDescriptor appDesc) {
 *     MessageStream<PageViewEvent> pageViewEvents =
 *       appDesc.getInputStream("pageViewEvents", (k, m) -> (PageViewEvent) m);
 *     OutputStream<String, PageViewEvent, PageViewEvent> recentPageViewEvents =
 *       appDesc.getOutputStream("recentPageViewEvents", m -> m.memberId, m -> m);
 *
 *     pageViewEvents
 *       .filter(m -> m.getCreationTime() > System.currentTimeMillis() - Duration.ofHours(1).toMillis())
 *       .sendTo(filteredPageViewEvents);
 *   }
 * }
 * }</pre>
 *<p>
 * The example above can be run using an ApplicationRunner:
 * <pre>{@code
 *   public static void main(String[] args) {
 *     CommandLine cmdLine = new CommandLine();
 *     Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
 *     PageViewCounter app = new PageViewCounter();
 *     ApplicationRunner runner = ApplicationRunners.getApplicationRunner(new PageViewCounter(), config);
 *     runner.run();
 *     runner.waitForFinish();
 *   }
 * }</pre>
 *
 * <p>
 * Implementation Notes: Currently StreamApplications are wrapped in a {@link org.apache.samza.task.StreamTask} during
 * execution. A new {@link StreamAppDescriptor} instance will be created and described by the user-defined
 * {@link StreamApplication} when planning the execution. All user-defined transformation functions and descriptors for
 * data entities used in the transformations (e.g. {@link org.apache.samza.operators.TableDescriptor}) are required to
 * be serializable. The execution planner will generate a serialized DAG which will be deserialized in each
 * {@link org.apache.samza.task.StreamTask} instance used for processing incoming messages. The user-defined application
 * class that implements {@link StreamApplication} also needs to be a class with proper fully-qualified class name to ensure
 * successful instantiation in both local and remote environments. Execution is synchronous and thread-safe within each
 * {@link org.apache.samza.task.StreamTask}.
 *
 * <p>
 * Functions implemented for transforms in StreamApplications ({@link org.apache.samza.operators.functions.MapFunction},
 * {@link org.apache.samza.operators.functions.FilterFunction} for e.g.) are initable and closable. They are initialized
 * before messages are delivered to them and closed after their execution when the {@link org.apache.samza.task.StreamTask}
 * instance is closed. See {@link org.apache.samza.operators.functions.InitableFunction} and {@link org.apache.samza.operators.functions.ClosableFunction}.
 */
@InterfaceStability.Evolving
public interface StreamApplication extends ApplicationBase<StreamAppDescriptor> {
}
