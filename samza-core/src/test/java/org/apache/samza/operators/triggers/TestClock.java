package org.apache.samza.operators.triggers;

import org.apache.samza.util.Clock;

import java.time.Duration;

/**
 * An implementation of {@link Clock} that allows to advance the time by an arbitrary duration.
 * Used for testing.
 */
public class TestClock implements Clock {

  long currentTime = 1;

  public void advanceTime(Duration duration) {
    currentTime += duration.toMillis();
  }

  @Override
  public long currentTimeMillis() {
    return currentTime;
  }
}
