package org.apache.samza.util;

import java.time.temporal.ChronoUnit;


public class RetryPolicyConfig {
  private final int maxRetries;
  private final long maxDuration;
  private final double jitterFactor;
  private final long backoffDelay;
  private final long backoffMaxDelay;
  private final int backoffDelayFactor;
  private final ChronoUnit unit;

  public RetryPolicyConfig(int maxRetries, long maxDuration, double jitterFactor, long backoffDelay, long backoffMaxDelay,
      int backoffDelayFactor, ChronoUnit unit) {
    this.maxRetries = maxRetries;
    this.maxDuration = maxDuration;
    this.jitterFactor = jitterFactor;
    this.backoffDelay = backoffDelay;
    this.backoffMaxDelay = backoffMaxDelay;
    this.backoffDelayFactor = backoffDelayFactor;
    this.unit = unit;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public ChronoUnit getUnit() {
    return unit;
  }

  public int getBackoffDelayFactor() {
    return backoffDelayFactor;
  }

  public long getBackoffMaxDelay() {
    return backoffMaxDelay;
  }

  public long getBackoffDelay() {
    return backoffDelay;
  }

  public double getJitterFactor() {
    return jitterFactor;
  }

  public long getMaxDuration() {
    return maxDuration;
  }
}
