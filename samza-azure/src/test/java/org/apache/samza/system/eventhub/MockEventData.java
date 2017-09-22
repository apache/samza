package org.apache.samza.system.eventhub;

import com.microsoft.azure.eventhubs.EventData;

import java.nio.charset.Charset;
import java.util.*;

public class MockEventData extends EventData {

    EventData.SystemProperties _overridedSystemProperties;

    public MockEventData(byte[] data, String partitionKey, String offset) {
        super(data);
        HashMap<String, Object> properties = new HashMap<>();
        properties.put("x-opt-offset", offset);
        properties.put("x-opt-partition-key", partitionKey);
        properties.put("x-opt-enqueued-time", new Date(System.currentTimeMillis()));
        _overridedSystemProperties = new SystemProperties(properties);
    }

    @Override
    public EventData.SystemProperties getSystemProperties() {
        return _overridedSystemProperties;
    }

    public static List<EventData> generateEventData(int numEvents) {
        Random rand = new Random(System.currentTimeMillis());
        List<EventData> result = new ArrayList<>();
        for (int i = 0; i < numEvents; i++) {
            String key = "key_" + rand.nextInt();
            String message = "message:" + rand.nextInt();
            String offset = "offset_" + i;
            EventData eventData = new MockEventData(message.getBytes(Charset.defaultCharset()), key, offset);
            result.add(eventData);
        }
        return result;
    }
}
