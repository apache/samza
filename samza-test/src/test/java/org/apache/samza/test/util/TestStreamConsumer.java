package org.apache.samza.test.util;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;

public class TestStreamConsumer implements SystemConsumer {
  private List<IncomingMessageEnvelope> envelopes;

  public TestStreamConsumer(List<IncomingMessageEnvelope> envelopes) {
    this.envelopes = envelopes;
  }

  @Override
  public void start() { }

  @Override
  public void stop() { }

  @Override
  public void register(SystemStreamPartition systemStreamPartition, String offset) { }

  @Override
  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(
      Set<SystemStreamPartition> systemStreamPartitions, long timeout)
      throws InterruptedException {
    return systemStreamPartitions.stream().collect(Collectors.toMap(ssp -> ssp, ssp -> envelopes));
  }
}
