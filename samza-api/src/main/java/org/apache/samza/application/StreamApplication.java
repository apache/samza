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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.operators.*;
import org.apache.samza.operators.functions.InitableFunction;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.task.StreamTask;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Describes and initializes the transforms for processing message streams and generating results.
 * <p>
 * The following example removes page views older than 1 hour from the input stream:
 * <pre>{@code
 * public class PageViewCounter implements StreamApplication {
 *   public void init(StreamGraph graph, Config config) {
 *     MessageStream<PageViewEvent> pageViewEvents =
 *       graph.getInputStream("pageViewEvents", (k, m) -> (PageViewEvent) m);
 *     OutputStream<String, PageViewEvent, PageViewEvent> recentPageViewEvents =
 *       graph.getOutputStream("recentPageViewEvents", m -> m.memberId, m -> m);
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
 *     LocalApplicationRunner runner = new LocalApplicationRunner(config);
 *     runner.run(app);
 *     runner.waitForFinish();
 *   }
 * }</pre>
 *
 * <p>
 * Implementation Notes: Currently StreamApplications are wrapped in a {@link StreamTask} during execution.
 * A new StreamApplication instance will be created and initialized when planning the execution, as well as for each
 * {@link StreamTask} instance used for processing incoming messages. Execution is synchronous and thread-safe within
 * each {@link StreamTask}.
 *
 * <p>
 * Functions implemented for transforms in StreamApplications ({@link org.apache.samza.operators.functions.MapFunction},
 * {@link org.apache.samza.operators.functions.FilterFunction} for e.g.) are initable and closable. They are initialized
 * before messages are delivered to them and closed after their execution when the {@link StreamTask} instance is closed.
 * See {@link InitableFunction} and {@link org.apache.samza.operators.functions.ClosableFunction}.
 */
@InterfaceStability.Unstable
public class StreamApplication {

  private final StreamGraph graph;
  private final ApplicationRunner runner;

  public static StreamApplication create(Config config) {
    ApplicationRunner runner = ApplicationRunner.fromConfig(config);

    return new StreamApplication(runner);

  }

  private StreamApplication(ApplicationRunner runner) {
    this.graph = runner.createGraph();
    this.runner = runner;
  }

  /**
   * Gets the input {@link MessageStream} corresponding to the {@code streamId}.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param <K> the type of key in the incoming message
   * @param <V> the type of message in the incoming message
   * @param <M> the type of message in the input {@link MessageStream}
   * @return the input {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  public <K, V, M> MessageStream<M> open(StreamDescriptor.Input<K, V> input, BiFunction<? super K, ? super V, ? extends M> msgBuilder) {
    return this.graph.getInputStream(input, msgBuilder);
  }

  /**
   * Gets the {@link OutputStream} corresponding to the {@code streamId}.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param <K> the type of key in the outgoing message
   * @param <V> the type of message in the outgoing message
   * @param <M> the type of message in the {@link OutputStream}
   * @return the output {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  public <K, V, M> OutputStream<K, V, M> open(StreamDescriptor.Output<K, V> output, Function<? super M, ? extends K> keyExtractor, Function<? super M, ? extends V> msgExtractor) {
    return this.graph.getOutputStream(output, keyExtractor, msgExtractor);
  }

  public StreamApplication withDefaultIntermediateSystem(IOSystem defaultSystem) {
    this.graph.setDefaultIntermediateSystem(defaultSystem);
    return this;
  }

  /**
   * Sets the {@link ContextManager} for this {@link StreamGraph}.
   * <p>
   * The provided {@link ContextManager} can be used to setup shared context between the operator functions
   * within a task instance
   *
   * @param contextManager the {@link ContextManager} to use for the {@link StreamGraph}
   */
  public StreamApplication withContextManager(ContextManager contextManager) {
    this.graph.setContextManager(contextManager);
    return this;
  }

  /**
   * Deploy and run the Samza jobs to execute {@link StreamApplication}.
   * It is non-blocking so it doesn't wait for the application running.
   *
   */
  public void run() {
    this.runner.run(this);
  }

  /**
   * Kill the Samza jobs represented by {@link StreamApplication}
   * It is non-blocking so it doesn't wait for the application stopping.
   *
   */
  public void kill() {
    this.runner.kill(this);
  }

  /**
   * Get the collective status of the Samza jobs represented by {@link StreamApplication}.
   * Returns {@link ApplicationRunner} running if all jobs are running.
   *
   * @return the status of the application
   */
  public ApplicationStatus status() {
    return this.runner.status(this);
  }

  /**
   * Wait till the current runner in the local JVM finishes, when returns, the stream application in the local JVM has
   * completed either successfully or with failure.
   *
   * <p>
   * Note this method returns as the runner in the current JVM finishes. If the runner is a local runner, it means that the
   * local stream application has finished; if the runner is a remote runner, it means that the stream application has
   * finished submitting to the cluster manager (e.g. YARN RM).
   * </p>
   */
  public void waitForFinish() {
    this.runner.waitForFinish();
  }

  public StreamApplication withMetricsReports(Map<String, MetricsReporter> reporterMap) {
    this.runner.setMetricsReporters(reporterMap);
    return this;
  }

}
