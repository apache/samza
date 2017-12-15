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
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.InitableFunction;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.serializers.Serde;
import org.apache.samza.task.StreamTask;

import java.util.Map;


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

  /*package private*/
  final ApplicationRunner runner;
  final Config config;
  final StreamGraph graph;

  StreamApplication(ApplicationRunner runner, Config config) {
    this.runner = runner;
    this.config = config;
    this.graph = runner.createGraph();
  }

  /**
   * Gets the input {@link MessageStream} corresponding to the {@code streamId}.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param <M> the type of input messages
   * @param streamId the input {@link StreamDescriptor.Input}
   * @param serde the {@link Serde} object used to deserialize input messages
   * @return the input {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   * @throws IOException when fail to create a serializable input operator to read the input messages
   */
  public <M> MessageStream<M> openInput(String streamId, Serde<M> serde) throws IOException {
    return this.graph.getInputStream(streamId, serde);
  }

  /**
   * Gets the input {@link MessageStream} corresponding to the {@code streamId}.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param <M> the type of message in the input {@link MessageStream}
   * @param streamId the input {@link StreamDescriptor.Input}
   * @return the input {@link MessageStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   * @throws IOException when fail to create a serializable input operator to read the input messages
   */
  public <M> MessageStream<M> openInput(String streamId) throws IOException {
    return this.graph.getInputStream(streamId);
  }

  /**
   * Gets the {@link OutputStream} corresponding to the {@code streamId}.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param <M> the type of message in the {@link OutputStream}
   * @param output the {@link StreamDescriptor.Output} to describe the {@code output} object
   * @param serde the {@link Serde} object used to serialize output messages
   * @return the output {@link OutputStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  public <M> OutputStream<M> openOutput(String output, Serde<M> serde) {
    return this.graph.getOutputStream(output, serde);
  }

  /**
   * Gets the {@link OutputStream} corresponding to the {@code streamId}.
   * <p>
   * Multiple invocations of this method with the same {@code streamId} will throw an {@link IllegalStateException}.
   *
   * @param <M> the type of message in the {@link OutputStream}
   * @param output the {@link StreamDescriptor.Output} to describe the {@code output} object
   * @return the output {@link OutputStream}
   * @throws IllegalStateException when invoked multiple times with the same {@code streamId}
   */
  public <M> OutputStream<M> openOutput(String output) {
    return this.graph.getOutputStream(output);
  }

  /**
   * Deploy and run the Samza jobs to execute this application.
   * It is non-blocking so it doesn't wait for the application running.
   *
   */
  public final void run() {
    this.runner.run(this);
  }

  /**
   * Kill the Samza jobs represented by this application
   * It is non-blocking so it doesn't wait for the application stopping.
   *
   */
  public final void kill() {
    this.runner.kill(this);
  }

  /**
   * Get the collective status of the Samza jobs represented by this application.
   * Returns {@link ApplicationStatus} running if all jobs are running.
   *
   * @return the status of the application
   */
  public final ApplicationStatus status() {
    return this.runner.status(this);
  }

  /**
   * Method to wait for the runner in the current JVM process to finish.
   */
  public final void waitForFinish() {
    this.runner.waitForFinish(this);
  }

  public StreamApplication withMetricsReporters(Map<String, MetricsReporter> metricsReporters) {
    this.runner.addMetricsReporters(metricsReporters);
    return this;
  }

  public class AppConfig extends MapConfig {

    public static final String APP_NAME = "app.name";
    public static final String APP_ID = "app.id";
    public static final String APP_CLASS = "app.class";

    public static final String JOB_NAME = "job.name";
    public static final String JOB_ID = "job.id";

    public AppConfig(Config config) {
      super(config);
    }

    public String getAppName() {
      return get(APP_NAME, get(JOB_NAME));
    }

    public String getAppId() {
      return get(APP_ID, get(JOB_ID, "1"));
    }

    public String getAppClass() {
      return get(APP_CLASS, null);
    }

    /**
     * returns full application id
     * @return full app id
     */
    public String getGlobalAppId() {
      return String.format("app-%s-%s", getAppName(), getAppId());
    }

  }


  public String getGlobalAppId() {
    return new AppConfig(config).getGlobalAppId();
  }

}
