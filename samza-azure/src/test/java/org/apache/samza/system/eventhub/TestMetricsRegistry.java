package org.apache.samza.system.eventhub;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestMetricsRegistry implements MetricsRegistry {

  private Map<String, List<Counter>> _counters = new HashedMap<>();
  private Map<String, List<Gauge<?>>> _gauges = new HashedMap<>();

  public List<Counter> getCounters(String groupName) {
    return _counters.get(groupName);
  }

  public List<Gauge<?>> getGauges(String groupName) {
    return _gauges.get(groupName);
  }

  @Override
  public Counter newCounter(String group, String name) {
    if (!_counters.containsKey(group)) {
      _counters.put(group, new ArrayList<>());
    }
    Counter c = new Counter(name);
    _counters.get(group).add(c);
    return c;
  }

  @Override
  public <T> Gauge<T> newGauge(String group, String name, T value) {
    if (!_gauges.containsKey(group)) {
      _gauges.put(group, new ArrayList<>());
    }

    Gauge<T> g = new Gauge<T>(name, value);
    _gauges.get(group).add(g);
    return g;
  }

  @Override
  public Counter newCounter(String group, Counter counter) {
    return null;
  }

  @Override
  public <T> Gauge<T> newGauge(String group, Gauge<T> value) {
    return null;
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
