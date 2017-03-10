package org.apache.samza.operators;

/**
 * Wraps the value for the window with additional metadata.
 */
public class WindowState<WV> {

  final WV wv;
  /**
   * Time of the first message in the window
   */
  final long earliestTime;

  public WindowState(WV wv, long earliestTime) {
    this.wv = wv;
    this.earliestTime = earliestTime;
  }

  public WV getWindowValue() {
    return wv;
  }

  public long getEarliestTime() {
    return earliestTime;
  }
}
