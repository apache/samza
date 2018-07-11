package org.apache.samza.application.internal;

import org.apache.samza.config.Config;
import org.apache.samza.operators.StreamGraph;


/**
 * Created by yipan on 7/10/18.
 */
public class StreamApplicationSpec extends ApplicationSpec {
  final StreamGraph graph;

  public StreamApplicationSpec(StreamGraph graph, Config config) {
    super(config);
    this.graph = graph;
  }
}
