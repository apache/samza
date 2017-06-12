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
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.operators.*;
import org.apache.samza.operators.functions.InitableFunction;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.task.StreamTask;

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
    StreamGraph graph = new StreamGraph() {
      @Override
      public <K, V, M> MessageStream<M> getInputStream(String streamId, BiFunction<? super K, ? super V, ? extends M> msgBuilder) {
        return null;
      }

      @Override
      public <K, V, M> OutputStream<K, V, M> getOutputStream(String streamId, Function<? super M, ? extends K> keyExtractor, Function<? super M, ? extends V> msgExtractor) {
        return null;
      }

      @Override
      public StreamGraph withContextManager(ContextManager contextManager) {
        return null;
      }
    };

    ApplicationRunner runner = ApplicationRunner.fromConfig(config);

    return new StreamApplication(graph, runner);

  }

  private StreamApplication(StreamGraph graph, ApplicationRunner runner) {
    this.graph = graph;
    this.runner = runner;
  }

  /**
   * Gets the input {@link MessageStream} corresponding to the {@code streamId}.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param streamId the unique ID for the stream
   * @param msgBuilder the {@link BiFunction} to convert the incoming key and message to a message
   *                   in the input {@link MessageStream}
   * @param <K> the type of key in the incoming message
   * @param <V> the type of message in the incoming message
   * @param <M> the type of message in the input {@link MessageStream}
   * @return the input {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  public <K, V, M> MessageStream<M> getInputStream(String streamId, BiFunction<? super K, ? super V, ? extends M> msgBuilder) {
    return this.graph.getInputStream(streamId, msgBuilder);
  }

  public <K, V, M> MessageStream<M> input(StreamIO.Input input, BiFunction<? super K, ? super V, ? extends M> msgBuilder) {
    return this.graph.getInputStream(input.getId(), msgBuilder);
  }

  /**
   * Gets the {@link OutputStream} corresponding to the {@code streamId}.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param streamId the unique ID for the stream
   * @param keyExtractor the {@link Function} to extract the outgoing key from the output message
   * @param msgExtractor the {@link Function} to extract the outgoing message from the output message
   * @param <K> the type of key in the outgoing message
   * @param <V> the type of message in the outgoing message
   * @param <M> the type of message in the {@link OutputStream}
   * @return the output {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  public <K, V, M> OutputStream<K, V, M> getOutputStream(String streamId,
    Function<? super M, ? extends K> keyExtractor, Function<? super M, ? extends V> msgExtractor) {
    return this.graph.getOutputStream(streamId, keyExtractor, msgExtractor);
  }

  public <K, V, M> OutputStream<K, V, M> output(StreamIO.Output output, Function<? super M, ? extends K> keyExtractor,
                                                Function<? super M, ? extends V> msgExtractor) {
    return this.graph.getOutputStream(output.getId(), keyExtractor, msgExtractor);
  }

  /**
   * Sets the {@link ContextManager} for this {@link StreamGraph}.
   * <p>
   * The provided {@link ContextManager} can be used to setup shared context between the operator functions
   * within a task instance
   *
   * @param contextManager the {@link ContextManager} to use for the {@link StreamGraph}
   */
  public void withContextManager(ContextManager contextManager) {
    this.graph.withContextManager(contextManager);
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

  public void waitForFinish() {
    this.runner.waitForFinish();
  }

  /**
   * Constructs a {@link StreamSpec} from the configuration for the specified streamId.
   *
   * The stream configurations are read from the following properties in the config:
   * {@code streams.{$streamId}.*}
   * <br>
   * All properties matching this pattern are assumed to be system-specific with two exceptions. The following two
   * properties are Samza properties which are used to bind the stream to a system and a physical resource on that system.
   *
   * <ul>
   *   <li>samza.system -         The name of the System on which this stream will be used. If this property isn't defined
   *                              the stream will be associated with the System defined in {@code job.default.system}</li>
   *   <li>samza.physical.name -  The system-specific name for this stream. It could be a file URN, topic name, or other identifer.
   *                              If this property isn't defined the physical.name will be set to the streamId</li>
   * </ul>
   *
   * @param streamId  The logical identifier for the stream in Samza.
   * @return          The {@link StreamSpec} instance.
   */
  public StreamSpec getStreamSpec(String streamId) {
    return this.runner.getStreamSpec(streamId);
  }

}
