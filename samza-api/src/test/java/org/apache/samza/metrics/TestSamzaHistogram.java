package org.apache.samza.metrics;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestSamzaHistogram {
  private static final String GROUP = "Group0";
  private static final String METRIC_NAME = "Metric1";

  private MockMetricRegistry metricsRegistry;

  @Test
  public void testCreateHistogramGaugeNullCheck() {
    metricsRegistry = new MockMetricRegistry();
    SamzaHistogram histogram = new SamzaHistogram(metricsRegistry, GROUP, METRIC_NAME);
    assertNotNull(histogram);
  }

  private class MockMetricRegistry implements MetricsRegistry {
    @Override
    public Counter newCounter(String group, String name) {
      return null;
    }

    @Override
    public Counter newCounter(String group, Counter counter) {
      return null;
    }

    @Override
    public <T> Gauge<T> newGauge(String group, String name, T value) {
      return null;
    }

    @Override
    public <T> Gauge<T> newGauge(String group, Gauge<T> value) {
      return new Gauge(group, value.getValue());
    }

    @Override
    public Timer newTimer(String group, String name) {
      return null;
    }

    @Override
    public Timer newTimer(String group, Timer timer) {
      return null;
    }
  }
}