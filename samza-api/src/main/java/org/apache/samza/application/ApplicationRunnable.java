package org.apache.samza.application;

import java.time.Duration;
import java.util.Map;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.InitableFunction;
import org.apache.samza.task.StreamTask;


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
 * A new StreamApplication instance will be created and initialized with a user-defined {@link StreamGraph}
 * when planning the execution. The {@link StreamGraph} and the functions implemented for transforms are required to
 * be serializable. The execution planner will generate a serialized DAG which will be deserialized in each {@link StreamTask}
 * instance used for processing incoming messages. Execution is synchronous and thread-safe within each {@link StreamTask}.
 *
 * <p>
 * Functions implemented for transforms in StreamApplications ({@link org.apache.samza.operators.functions.MapFunction},
 * {@link org.apache.samza.operators.functions.FilterFunction} for e.g.) are initable and closable. They are initialized
 * before messages are delivered to them and closed after their execution when the {@link StreamTask} instance is closed.
 * See {@link InitableFunction} and {@link org.apache.samza.operators.functions.ClosableFunction}.
 */
@InterfaceStability.Evolving
public interface ApplicationRunnable {

  void run();

  void kill();

  ApplicationStatus status();

  void waitForFinish();

  boolean waitForFinish(Duration timeout);

  /**
   * Set {@link MetricsReporter}s for this {@link StreamApplications.ApplicationRuntimeInstance}
   *
   * @param metricsReporters the map of {@link MetricsReporter}s to be added
   * @return this {@link StreamApplications.ApplicationRuntimeInstance} instance
   */
  ApplicationRunnable withMetricsReporters(Map<String, MetricsReporter> metricsReporters);

}
