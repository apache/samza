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
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;


/**
 * A {@link TaskApplication} describes the inputs, outputs, state, configuration and the processing logic
 * in Samza's Low Level API.
 * A typical {@link TaskApplication} implementation consists of the following stages:
 * <ol>
 *   <li>Configuring the inputs, outputs and state (tables) using the appropriate
 *   {@link org.apache.samza.system.descriptors.SystemDescriptor}s,
 *   {@link org.apache.samza.system.descriptors.StreamDescriptor}s and
 *   {@link org.apache.samza.table.descriptors.TableDescriptor}s
 *   <li>Adding these descriptors to the provided {@link TaskApplicationDescriptor}.
 *   <li>Defining the processing logic by implementing a {@link org.apache.samza.task.StreamTask} or
 *   {@link org.apache.samza.task.AsyncStreamTask} that operates on each
 *   {@link org.apache.samza.system.IncomingMessageEnvelope} one at a time.
 *   <li>Setting a {@link org.apache.samza.task.TaskFactory} using
 *   {@link TaskApplicationDescriptor#withTaskFactory(org.apache.samza.task.TaskFactory)} that creates instances of the
 *   task above. The {@link org.apache.samza.task.TaskFactory} implementation must be {@link java.io.Serializable}.
 * </ol>
 * <p>
 * The following example {@link TaskApplication} removes page views older than 1 hour from the input stream:
 * <pre>{@code
 * public class PageViewFilter implements TaskApplication {
 *   public void describe(TaskApplicationDescriptor appDescriptor) {
 *     KafkaSystemDescriptor trackingSystemDescriptor = new KafkaSystemDescriptor("tracking");
 *     KafkaInputDescriptor<PageViewEvent> inputStreamDescriptor =
 *         trackingSystemDescriptor.getInputDescriptor("pageViewEvent", new JsonSerdeV2<>(PageViewEvent.class));
 *     KafkaOutputDescriptor<PageViewEvent>> outputStreamDescriptor =
 *         trackingSystemDescriptor.getOutputDescriptor("recentPageViewEvent", new JsonSerdeV2<>(PageViewEvent.class)));
 *
 *     appDescriptor
 *         .withInputStream(inputStreamDescriptor)
 *         .withOutputStream(outputStreamDescriptor)
 *         .withTaskFactory((StreamTaskFactory) () -> new PageViewTask());
 *   }
 * }
 *
 * public class PageViewTask implements StreamTask {
 *   public void process(IncomingMessageEnvelope message, MessageCollector collector, TaskCoordinator coordinator) {
 *     PageViewEvent m = (PageViewEvent) message.getValue();
 *     if (m.getCreationTime() > System.currentTimeMillis() - Duration.ofHours(1).toMillis()) {
 *       collector.send(new OutgoingMessageEnvelope(
 *          new SystemStream("tracking", "recentPageViewEvent"), message.getKey(), message.getKey(), m));
 *     }
 *   }
 * }
 * }</pre>
 */
@InterfaceStability.Evolving
public interface TaskApplication extends SamzaApplication<TaskApplicationDescriptor> {
}