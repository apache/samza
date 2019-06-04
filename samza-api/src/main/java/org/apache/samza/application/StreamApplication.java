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

import java.io.Serializable;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.ClosableFunction;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.operators.functions.InitableFunction;
import org.apache.samza.operators.functions.ScheduledFunction;
import org.apache.samza.system.descriptors.InputDescriptor;
import org.apache.samza.system.descriptors.OutputDescriptor;
import org.apache.samza.system.descriptors.SystemDescriptor;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.task.StreamTask;


/**
 * A {@link StreamApplication} describes the inputs, outputs, state, configuration and the processing logic for the
 * application in Samza's High Level API.
 * <p>
 * A typical {@link StreamApplication} implementation consists of the following stages:
 * <ol>
 *   <li>Configuring the inputs, outputs and state (tables) using the appropriate {@link SystemDescriptor}s,
 *       {@link InputDescriptor}s, {@link OutputDescriptor}s and {@link TableDescriptor}s
 *   <li>Obtaining the corresponding {@link MessageStream}s, {@link OutputStream}s and {@link Table}s from the
 *       provided {@link StreamApplicationDescriptor}.
 *   <li>Defining the processing logic using operators and functions on the streams and tables thus obtained.
 *       E.g., {@link MessageStream#filter(FilterFunction)}
 * </ol>
 * <p>
 * The following example {@link StreamApplication} removes page views older than 1 hour from the input stream:
 * <pre>{@code
 * public class PageViewFilter implements StreamApplication {
 *   public void describe(StreamApplicationDescriptor appDescriptor) {
 *     KafkaSystemDescriptor trackingSystemDescriptor = new KafkaSystemDescriptor("tracking");
 *     KafkaInputDescriptor<PageViewEvent> inputStreamDescriptor =
 *         trackingSystemDescriptor.getInputDescriptor("pageViewEvent", new JsonSerdeV2<>(PageViewEvent.class));
 *     KafkaOutputDescriptor<PageViewEvent>> outputStreamDescriptor =
 *         trackingSystemDescriptor.getOutputDescriptor("recentPageViewEvent", new JsonSerdeV2<>(PageViewEvent.class)));
 *
 *     MessageStream<PageViewEvent> pageViewEvents = appDescriptor.getInputStream(inputStreamDescriptor);
 *     OutputStream<PageViewEvent> recentPageViewEvents = appDescriptor.getOutputStream(outputStreamDescriptor);
 *
 *     pageViewEvents
 *       .filter(m -> m.getCreationTime() > System.currentTimeMillis() - Duration.ofHours(1).toMillis())
 *       .sendTo(recentPageViewEvents);
 *   }
 * }
 * }</pre>
 * <p>
 * All operator function implementations used in a {@link StreamApplication} must be {@link Serializable}. Any
 * context required within an operator function may be managed by implementing the {@link InitableFunction#init} and
 * {@link ClosableFunction#close} methods in the function implementation.
 * <p>
 * Functions may implement the {@link ScheduledFunction} interface to schedule and receive periodic callbacks from the
 * Samza framework.
 * <p>
 * Implementation Notes: Currently {@link StreamApplication}s are wrapped in a {@link StreamTask} during execution. The
 * execution planner will generate a serialized DAG which will be deserialized in each {@link StreamTask} instance used
 * for processing incoming messages. Execution is synchronous and thread-safe within each {@link StreamTask}. Multiple
 * tasks may process their messages concurrently depending on the job parallelism configuration.
 */
@InterfaceStability.Evolving
public interface StreamApplication extends SamzaApplication<StreamApplicationDescriptor> {
}
