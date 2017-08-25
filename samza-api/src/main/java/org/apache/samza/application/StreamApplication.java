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

import java.io.IOException;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.IOSystem;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.InitableFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.OperatorBiFunction;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.task.StreamTask;

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
public class StreamApplication extends ApplicationBase {

  /*package private*/
  final StreamGraph graph;

  StreamApplication(ApplicationRunner runner, Config config) {
    super(runner, config);
    this.graph = runner.createGraph();
  }

  /**
   * Gets the input {@link MessageStream} corresponding to the {@code streamId}.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param <K> the type of key in the incoming message
   * @param <V> the type of message in the incoming message
   * @param input the input {@link StreamDescriptor.Input}
   * @return the input {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  public <K, V> MessageStream<V> openInput(StreamDescriptor.Input<K, V> input) throws IOException {
    return this.graph.getInputStream(input, (k, v) -> v);
  }

  /**
   * Gets the input {@link MessageStream} corresponding to the {@code streamId}.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param <K> the type of key in the incoming message
   * @param <V> the type of message in the incoming message
   * @param <M> the type of message in the input {@link MessageStream}
   * @param input the input {@link StreamDescriptor.Input}
   * @param msgBuilder the function to construct a message of type M from the key-value pair
   * @return the input {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  public <K, V, M> MessageStream<M> openInput(
      StreamDescriptor.Input<K, V> input,
      OperatorBiFunction<? super K, ? super V, ? extends M> msgBuilder) throws IOException {
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
   * @param output the {@link StreamDescriptor.Output} to describe the {@code output} object
   * @param keyExtractor the function to get the key from the message
   * @param msgExtractor the function to get the value from the message
   * @return the output {@link OutputStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  public <K, V, M> OutputStream<K, V, M> openOutput(
      StreamDescriptor.Output<K, V> output,
      MapFunction<? super M, ? extends K> keyExtractor,
      MapFunction<? super M, ? extends V> msgExtractor) {
    return this.graph.getOutputStream(output, keyExtractor, msgExtractor);
  }

  /**
   * Gets the {@link OutputStream} corresponding to the {@code streamId}.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param <K> the type of key in the outgoing message
   * @param <V> the type of message in the outgoing message
   * @param output the {@link StreamDescriptor.Output} to describe the {@code output} object
   * @param keyExtractor the function to get the key from the message
   * @return the output {@link OutputStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  public <K, V> OutputStream<K, V, V> openOutput(
      StreamDescriptor.Output<K, V> output,
      MapFunction<? super V, ? extends K> keyExtractor) {
    return this.graph.getOutputStream(output, keyExtractor, v -> v);
  }

  public StreamApplication withDefaultSystem(IOSystem defaultSystem) {
    this.graph.setDefaultSystem(defaultSystem);
    return this;
  }

  public StreamApplication withMetricsReporters(Map<String, MetricsReporter> metrics) {
    super.withMetricsReports(metrics);
    return this;
  }
}
