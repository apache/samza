package org.apache.samza.operators.functions;

/**
 * A fold function that incrementally combines and aggregates values for a window.
 */
public interface FoldFunction<M, WV> extends InitableFunction {

  /**
   * Incrementally combine and aggregate values for the window. Guaranteed to be invoked for every
   * message added to the window.
   *
   * @param message the incoming message that is added to the window
   * @param oldValue the previous value
   * @return the new value
   */
  WV apply(M message, WV oldValue);
}
