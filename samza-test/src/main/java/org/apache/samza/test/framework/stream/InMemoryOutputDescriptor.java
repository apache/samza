package org.apache.samza.test.framework.stream;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.InMemorySystemConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.descriptors.base.stream.OutputDescriptor;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.inmemory.InMemorySystemFactory;
import org.apache.samza.system.kafka.KafkaSystemDescriptor;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.InMemorySystemDescriptor;

/**
 * A descriptor for a In Memory output stream.
 * <p>
 * An instance of this descriptor may be obtained from an appropriately configured {@link InMemorySystemDescriptor}.
 * <p>
 * Stream properties configured using a descriptor override corresponding properties provided in configuration.
 *
 * @param <StreamMessageType> type of messages in this stream.
 */

public class InMemoryOutputDescriptor<StreamMessageType>
    extends OutputDescriptor<StreamMessageType, InMemoryOutputDescriptor<StreamMessageType>> {

  private Integer partitionCount;

  /**
   * Constructs an {@link OutputDescriptor} instance.
   * @param streamId id of the stream
   * @param systemDescriptor system descriptor this stream descriptor was obtained from
   */
  public InMemoryOutputDescriptor(String streamId, SystemDescriptor systemDescriptor) {
    super(streamId, new NoOpSerde<>(), systemDescriptor);
  }

  public InMemoryOutputDescriptor<StreamMessageType> withPartitionCount(Integer partitionCount) {
    this.partitionCount = partitionCount;
    addOutputStream();
    return this;
  }

  public Integer getPartitionCount() { return this.partitionCount; }

  @Override
  public Map<String, String> toConfig() {
    HashMap<String, String> configs = new HashMap<>(super.toConfig());
    InMemorySystemDescriptor descriptor = (InMemorySystemDescriptor) getSystemDescriptor();
    configs.put(InMemorySystemConfig.INMEMORY_SCOPE, descriptor.getInMemoryScope());
    return configs;
  }

  /**
   * Configures {@code stream} with the TestRunner, adds all the stream specific configs to global job configs.
   * <p>
   * Every stream belongs to a System (here a {@link InMemorySystemDescriptor}), this utility also registers the system with
   * {@link TestRunner} if not registered already. Then it creates the stream partitions with the registered System
   * <p>
   */
  public void addOutputStream() {
    InMemorySystemFactory factory = new InMemorySystemFactory();
    String physicalName = (String) getPhysicalName().orElse(getStreamId());
    StreamSpec spec = new StreamSpec(getStreamId(), physicalName, getSystemName(), this.partitionCount);
    factory
        .getAdmin(getSystemName(), new MapConfig(toConfig()))
        .createStream(spec);
  }

}
