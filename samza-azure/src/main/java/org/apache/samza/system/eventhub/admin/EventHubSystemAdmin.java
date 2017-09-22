package org.apache.samza.system.eventhub.admin;

import com.microsoft.azure.eventhubs.EventHubRuntimeInformation;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.eventhub.EventHubClientWrapper;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.apache.samza.system.eventhub.consumer.EventHubSystemConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class EventHubSystemAdmin implements SystemAdmin {
    private static final Logger LOG = LoggerFactory.getLogger(EventHubSystemAdmin.class);

    private String _systemName;
    private EventHubConfig _config;
    private Map<String, EventHubClientWrapper> _eventHubClients = new HashMap<>();

    public EventHubSystemAdmin(String systemName, EventHubConfig config) {
         _systemName = systemName;
         _config = config;
    }

    private static String getNextOffset(String currentOffset) {
        // EventHub will return the first message AFTER the offset
        // that was specified in the fetch request.
        return currentOffset.equals(EventHubSystemConsumer.END_OF_STREAM) ? currentOffset :
                String.valueOf(Long.parseLong(currentOffset) + 1);
    }

    @Override
    public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
        Map<SystemStreamPartition, String> results = new HashMap<>();
        offsets.forEach((partition, offset) -> results.put(partition, getNextOffset(offset)));
        return results;
    }

    @Override
    public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
        Map<String, SystemStreamMetadata> requestedMetadata = new HashMap<>();
        Map<String, CompletableFuture<EventHubRuntimeInformation>> ehRuntimeInfos = new HashMap<>();
        streamNames.forEach((streamName) -> {
            if (!_eventHubClients.containsKey(streamName)) {
                addEventHubClient(streamName);
            }
            ehRuntimeInfos.put(streamName,
                    _eventHubClients.get(streamName).getEventHubClient().getRuntimeInformation());
        });
        ehRuntimeInfos.forEach((streamName, ehRuntimeInfo) -> {
            try {
                EventHubRuntimeInformation ehInfo = ehRuntimeInfo.get(); // TODO: timeout
                Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> sspMetadataMap = new HashMap<>();
                for (String partition : ehInfo.getPartitionIds()) { //TODO getPartitionRuntimeInformation
                    sspMetadataMap.put(new Partition(Integer.parseInt(partition)),
                            new SystemStreamMetadata.SystemStreamPartitionMetadata(PartitionReceiver.START_OF_STREAM,
                                    EventHubSystemConsumer.END_OF_STREAM, EventHubSystemConsumer.END_OF_STREAM));
                }
                requestedMetadata.put(streamName, new SystemStreamMetadata(streamName, sspMetadataMap));
            } catch (Exception e){
                String msg = String.format("Error while fetching EventHubRuntimeInfo for System:%s, Stream:%s",
                        _systemName, streamName);
                LOG.error(msg);
                throw new SamzaException(msg);
            }
        });
        return requestedMetadata;
    }

    private void addEventHubClient(String streamName) {
        String ehNamespace = _config.getStreamNamespace(streamName);
        String ehEntityPath = _config.getStreamEntityPath(streamName);
        _eventHubClients.put(streamName, new EventHubClientWrapper(null, 0,
                ehNamespace, ehEntityPath, _config.getStreamSasKeyName(streamName), _config.getStreamSasToken(streamName)));
    }

    @Override
    public void createChangelogStream(String streamName, int numOfPartitions) {
        throw new UnsupportedOperationException("Event Hubs does not support change log stream.");
    }

    @Override
    public void validateChangelogStream(String streamName, int numOfPartitions) {
        throw new UnsupportedOperationException("Event Hubs does not support change log stream.");
    }

    @Override
    public void createCoordinatorStream(String streamName) {
        throw new UnsupportedOperationException("Event Hubs does not support coordinator stream.");
    }

    @Override
    public Integer offsetComparator(String offset1, String offset2) {
        try {
            if (offset1.equals(EventHubSystemConsumer.END_OF_STREAM)) {
                return offset2.equals(EventHubSystemConsumer.END_OF_STREAM) ? 0 : 1;
            }
            return offset2.equals(EventHubSystemConsumer.END_OF_STREAM) ? -1 :
                    Long.compare(Long.parseLong(offset1), Long.parseLong(offset2));
        } catch (NumberFormatException exception) {
            return null;
        }
    }
}
