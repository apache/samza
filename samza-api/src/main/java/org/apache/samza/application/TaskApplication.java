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
 * Describes and initializes the transforms for processing message streams and generating results in low-level API.
 * <p>
 * This is a marker interface that users will implement for a low-level application.
 * <p>
 * The following example removes page views older than 1 hour from the input stream:
 * <pre>{@code
 * public class PageViewCounter implements TaskApplication {
 *   public void describe(TaskAppDescriptor appDesc) {
 *     appDesc.addInputStream(PageViewTask.TASK_INPUT);
 *     appDesc.addOutputStream(PageViewTask.TASK_OUTPUT);
 *     appDesc.setTaskFactory((StreamTaskFactory) () -> new PageViewTask());
 *   }
 * }
 *
 * public class PageViewTask implements StreamTask {
 *   final static String TASK_INPUT = "pageViewEvents";
 *   final static String TASK_OUTPUT = "recentPageViewEvents";
 *   final static String OUTPUT_SYSTEM = "kafka";
 *
 *   public void process(IncomingMessageEnvelope message, MessageCollector collector,
 *       TaskCoordinator coordinator) {
 *     PageViewEvent m = (PageViewEvent) message.getValue();
 *     if (m.getCreationTime() > System.currentTimeMillis() - Duration.ofHours(1).toMillis()) {
 *       collector.send(new OutgoingMessageEnvelope(new SystemStream(OUTPUT_SYSTEM, TASK_OUTPUT),
 *           message.getKey(), message.getKey(), m));
 *     }
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
 * Implementation Notes: {@link TaskApplication} allow users to instantiate {@link org.apache.samza.task.StreamTask} or
 * {@link org.apache.samza.task.AsyncStreamTask} when describing the processing logic. A new {@link TaskAppDescriptor}
 * instance will be created and described by the user-defined {@link TaskApplication} when planning the execution.
 * {@link org.apache.samza.task.TaskFactory} and descriptors for data entities used in the task (e.g.
 * {@link org.apache.samza.operators.TableDescriptor}) are required to be serializable.
 *
 * <p>
 * The user-implemented {@link TaskApplication} class must be a class with proper fully-qualified class name and
 * a default constructor with no parameters to ensure successful instantiation in both local and remote environments.
 */
@InterfaceStability.Evolving
public interface TaskApplication extends SamzaApplication<TaskAppDescriptor> {
}