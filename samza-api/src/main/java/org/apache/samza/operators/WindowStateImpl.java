package org.apache.samza.operators;

import org.apache.samza.operators.WindowState;

/**
 * Created by jvenkatr on 10/6/16.
 */
//public class WindowStateImpl<WV> implements WindowState<WV>{

public class WindowStateImpl<WV> implements WindowState<WV> {

  private long firstSystemTimeNanos;
  private long lastSystemTimeNanos;
  private long earliestEventTimeNanos;
  private long latestEventTimeNanos;
  private long numMessages;
  private WV outputValue;


  @Override
  public long getFirstMessageTimeNs() {
    return firstSystemTimeNanos;
  }

  @Override
  public long getLastMessageTimeNs() {
    return lastSystemTimeNanos;
  }

  @Override
  public long getEarliestEventTimeNs() {
    return earliestEventTimeNanos;
  }

  @Override
  public long getLatestEventTimeNs() {
    return latestEventTimeNanos;
  }

  @Override
  public long getNumberMessages() {
    return numMessages;
  }

  @Override
  public WV getOutputValue() {
    return outputValue;
  }

  @Override
  public void setOutputValue(WV value) {
    this.outputValue = value;
  }

  public WindowStateImpl(long firstSystemTimeNanos, long lastSystemTimeNanos, long earliestEventTimeNanos, long latestEventTimeNanos, long numMessages, WV outputValue) {
    this.firstSystemTimeNanos = firstSystemTimeNanos;
    this.lastSystemTimeNanos = lastSystemTimeNanos;
    this.earliestEventTimeNanos = earliestEventTimeNanos;
    this.latestEventTimeNanos = latestEventTimeNanos;
    this.numMessages = numMessages;
    this.outputValue = outputValue;
  }

  @Override
  public String toString() {
    return "WindowStateImpl{" +
        "firstSystemTimeNanos=" + firstSystemTimeNanos +
        ", lastSystemTimeNanos=" + lastSystemTimeNanos +
        ", earliestEventTimeNanos=" + earliestEventTimeNanos +
        ", latestEventTimeNanos=" + latestEventTimeNanos +
        ", numMessages=" + numMessages +
        ", outputValue=" + outputValue +
        '}';
  }
}
