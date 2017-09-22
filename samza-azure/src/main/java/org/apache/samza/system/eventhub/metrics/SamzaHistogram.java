package org.apache.samza.system.eventhub.metrics;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;



public class SamzaHistogram {
    private final MetricsRegistry _registry;
    private final Histogram _histogram;
    private final List<Double> _percentiles;
    private final Map<Double, Gauge<Double>> _gauges;
    private static final List<Double> DEFAULT_HISTOGRAM_PERCENTILES = Arrays.asList(50D, 99D);

    public SamzaHistogram(MetricsRegistry registry, String group, String name) {
        this(registry, group, name, DEFAULT_HISTOGRAM_PERCENTILES);
    }

    public SamzaHistogram(MetricsRegistry registry, String group, String name, List<Double> percentiles) {
        _registry = registry;
        _histogram = new Histogram(new ExponentiallyDecayingReservoir());
        _percentiles = percentiles;
        _gauges = _percentiles.stream()
                .filter(x -> x > 0 && x <= 100)
                .collect(
                        Collectors.toMap(Function.identity(), x -> _registry.newGauge(group, name + "_" + String.valueOf(0), 0D)));
    }

    public void update(long value) {
        _histogram.update(value);
        Snapshot values = _histogram.getSnapshot();
        _percentiles.stream().forEach(x -> _gauges.get(x).set(values.getValue(x / 100)));
    }
}
