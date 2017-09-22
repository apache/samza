package org.apache.samza.system.eventhub.producer;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.servicebus.ServiceBusException;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.eventhub.EventHubClientWrapper;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.apache.samza.system.eventhub.EventHubSystemFactory;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class TestEventHubSystemProducer {
    private static final Logger LOG = LoggerFactory.getLogger(TestEventHubSystemProducer.class.getName());

    public static final String EVENTHUB_NAMESPACE = "";
    public static final String EVENTHUB_ENTITY1 = "";
    public static final String EVENTHUB_KEY_NAME = "";
    public static final String EVENTHUB_KEY = "";

    public static final String SYSTEM_NAME = "system1";
    public static final String STREAM_NAME1 = "test_stream1";
    public static final String STREAM_NAME2 = "test_stream2";

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
        return createEventHubConfig(EventHubClientWrapper.PartitioningMethod.EVENT_HUB_HASHING);
    }

    private Config createEventHubConfig(EventHubClientWrapper.PartitioningMethod partitioningMethod) {
        HashMap<String, String> mapConfig = new HashMap<>();
        mapConfig.put(EventHubSystemProducer.CONFIG_PARTITIONING_METHOD, partitioningMethod.toString());
        mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_LIST, SYSTEM_NAME), STREAM_NAME1);
        mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, SYSTEM_NAME, STREAM_NAME1), EVENTHUB_NAMESPACE);
        mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, SYSTEM_NAME, STREAM_NAME1), EVENTHUB_ENTITY1);
        mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, SYSTEM_NAME, STREAM_NAME1), EVENTHUB_KEY_NAME);
        mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, SYSTEM_NAME, STREAM_NAME1), EVENTHUB_KEY);
        mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_CONSUMER_START_POSITION, SYSTEM_NAME, STREAM_NAME1), "earliest");

        mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_LIST, SYSTEM_NAME), STREAM_NAME2);
        mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, SYSTEM_NAME, STREAM_NAME2), EVENTHUB_NAMESPACE);
        mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, SYSTEM_NAME, STREAM_NAME2), EVENTHUB_ENTITY1);
        mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, SYSTEM_NAME, STREAM_NAME2), EVENTHUB_KEY_NAME);
        mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, SYSTEM_NAME, STREAM_NAME2), EVENTHUB_KEY);
        mapConfig.put(String.format(EventHubConfig.CONFIG_STREAM_CONSUMER_START_POSITION, SYSTEM_NAME, STREAM_NAME2), "earliest");

        return new MapConfig(mapConfig);
    }

    @Test
    public void testSend() {
        Config eventHubConfig = createEventHubConfig();
        EventHubSystemFactory systemFactory = new EventHubSystemFactory();
        SystemProducer systemProducer = systemFactory.getProducer("system1", eventHubConfig, new NoOpMetricsRegistry());

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
        EventHubClientWrapper wrapper =
                new EventHubClientWrapper(EventHubClientWrapper.PartitioningMethod.EVENT_HUB_HASHING, 8, EVENTHUB_NAMESPACE,
                        EVENTHUB_ENTITY1, EVENTHUB_KEY_NAME, EVENTHUB_KEY);
        EventHubClient client = wrapper.getEventHubClient();
        PartitionReceiver receiver =
                client.createReceiverSync(EventHubClient.DEFAULT_CONSUMER_GROUP_NAME, "0",
                        "163456", true);
        receiveMessages(receiver, 300);
    }

    private void receiveMessages(PartitionReceiver receiver, int numMessages) throws ServiceBusException {
        int count = 0;
        while (count < numMessages) {

            Iterable<EventData> messages = receiver.receiveSync(100);
            if (messages == null) {
                System.out.println("End of stream");
                break;
            }
            for (EventData data : messages) {
                count++;
                LOG.info("Data" + new String(data.getBody()));
                System.out.println("\nDATA: " + new String(data.getBody()));
                System.out.println("BYTES_SIZE: " + data.getBytes().length);
                System.out.println("OFFSET: " + data.getSystemProperties().getOffset());
            }
        }
    }

    @Test
    public void testSendToSpecificPartition() {
        Config eventHubConfig = createEventHubConfig(EventHubClientWrapper.PartitioningMethod.PARTITION_KEY_AS_PARTITION);
        EventHubSystemFactory systemFactory = new EventHubSystemFactory();
        SystemProducer systemProducer = systemFactory.getProducer("system1", eventHubConfig, new NoOpMetricsRegistry());

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
                (EventHubSystemProducer) systemFactory.getProducer("system1", eventHubConfig, new NoOpMetricsRegistry());
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
