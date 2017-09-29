package org.apache.samza.system.eventhub.producer;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.servicebus.ServiceBusException;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.eventhub.*;
import org.apache.samza.system.eventhub.consumer.EventHubSystemConsumer;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.system.eventhub.MockEventHubConfigFactory.*;

public class TestEventHubSystemProducer {
  private static final Logger LOG = LoggerFactory.getLogger(TestEventHubSystemProducer.class.getName());

  @Test
  public void testSystemFactoryCreateAndStartProducer() {
    Config eventHubConfig = createEventHubConfig();
    EventHubSystemFactory systemFactory = new EventHubSystemFactory();
    SystemProducer systemProducer = systemFactory.getProducer(SYSTEM_NAME, eventHubConfig, new NoOpMetricsRegistry());
    Assert.assertNotNull(systemProducer);

    systemProducer.register(STREAM_NAME1);
    systemProducer.register(STREAM_NAME2);
    systemProducer.start();
    systemProducer.stop();
  }

  private Config createEventHubConfig() {
    return MockEventHubConfigFactory.getEventHubConfig(EventHubSystemProducer.PartitioningMethod.EVENT_HUB_HASHING);
  }

  @Test
  public void testSend() {
    Config eventHubConfig = createEventHubConfig();
    EventHubSystemFactory systemFactory = new EventHubSystemFactory();
    SystemProducer systemProducer = systemFactory.getProducer(SYSTEM_NAME, eventHubConfig, new NoOpMetricsRegistry());

    systemProducer.register(STREAM_NAME1);

    try {
      systemProducer.send(STREAM_NAME1, createMessageEnvelope(STREAM_NAME1));
      Assert.fail("Sending event before starting producer should throw exception");
    } catch (SamzaException e) {
    }

    systemProducer.start();
    systemProducer.send(STREAM_NAME1, createMessageEnvelope(STREAM_NAME1));

    try {
      systemProducer.send(STREAM_NAME2, createMessageEnvelope(STREAM_NAME1));
      Assert.fail("Sending event to destination that is not registered should throw exception");
    } catch (SamzaException e) {
    }

    try {
      systemProducer.register(STREAM_NAME2);
      Assert.fail("Trying to register after starting producer should throw exception");
    } catch (SamzaException e) {
    }

    systemProducer.flush(STREAM_NAME1);
    systemProducer.stop();
  }

  @Test
  public void testReceive() throws ServiceBusException {
    EventHubClientFactory clientFactory = new EventHubClientFactory();
    EventHubClientWrapper wrapper = clientFactory
            .getEventHubClient(EVENTHUB_NAMESPACE, EVENTHUB_ENTITY1, EVENTHUB_KEY_NAME, EVENTHUB_KEY,
                    new EventHubConfig(createEventHubConfig(), SYSTEM_NAME));
    wrapper.init();
    EventHubClient client = wrapper.getEventHubClient();
    PartitionReceiver receiver =
            client.createReceiverSync(EventHubClient.DEFAULT_CONSUMER_GROUP_NAME, "0",
                    EventHubSystemConsumer.START_OF_STREAM, true);
    receiveMessages(receiver, 300);
  }

  private void receiveMessages(PartitionReceiver receiver, int numMessages) throws ServiceBusException {
    int count = 0;
    while (count < numMessages) {

      Iterable<EventData> messages = receiver.receiveSync(100);
      if (messages == null) {
        break;
      }
      for (EventData data : messages) {
        count++;
        LOG.info("Data" + new String(data.getBody()));
      }
    }
  }

  @Test
  public void testSendToSpecificPartition() {
    Config eventHubConfig = MockEventHubConfigFactory.getEventHubConfig(EventHubSystemProducer.PartitioningMethod.PARTITION_KEY_AS_PARTITION);
    EventHubSystemFactory systemFactory = new EventHubSystemFactory();
    SystemProducer systemProducer = systemFactory.getProducer(SYSTEM_NAME, eventHubConfig, new NoOpMetricsRegistry());

    systemProducer.register(STREAM_NAME1);
    systemProducer.start();
    for (int i = 0; i < 100; i++) {
      systemProducer.send(STREAM_NAME1, createMessageEnvelope(STREAM_NAME1, 0));
    }
    systemProducer.flush(STREAM_NAME1);
    systemProducer.stop();
  }

  private OutgoingMessageEnvelope createMessageEnvelope(String streamName, int partition) {
    return new OutgoingMessageEnvelope(new SystemStream(SYSTEM_NAME, streamName), partition, "key1".getBytes(),
            "value0".getBytes());
  }

  private OutgoingMessageEnvelope createMessageEnvelope(String streamName) {
    return new OutgoingMessageEnvelope(new SystemStream(SYSTEM_NAME, streamName), "key1".getBytes(),
            "value".getBytes());
  }

  @Test
  public void testFlush() {
    Config eventHubConfig = createEventHubConfig();
    EventHubSystemFactory systemFactory = new EventHubSystemFactory();
    EventHubSystemProducer systemProducer =
            (EventHubSystemProducer) systemFactory.getProducer(SYSTEM_NAME, eventHubConfig, new NoOpMetricsRegistry());
    systemProducer.register(STREAM_NAME1);
    systemProducer.register(STREAM_NAME2);
    systemProducer.start();
    int numEvents = 100;
    for (int i = 0; i < numEvents; i++) {
      systemProducer.send(STREAM_NAME1, createMessageEnvelope(STREAM_NAME1));
      systemProducer.send(STREAM_NAME2, createMessageEnvelope(STREAM_NAME2));
    }
    systemProducer.flush(EVENTHUB_ENTITY1);
    Assert.assertEquals(systemProducer.getPendingFutures().size(), 0);
    systemProducer.stop();
  }
}
