package org.apache.samza.application;

import org.apache.samza.operators.StreamGraphImpl;


/**
 * Created by yipan on 7/27/17.
 */
public class StreamApplicationInternal {
  private final StreamApplication app;

  public StreamApplicationInternal(StreamApplication app) {
    this.app = app;
  }

  public StreamGraphImpl getStreamGraphImpl() {
    return (StreamGraphImpl) this.app.graph;
  }
}
