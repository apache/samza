package org.apache.samza.system.eventhub;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.eventhub.admin.EventHubSystemAdmin;
import org.apache.samza.system.eventhub.consumer.EventHubEntityConnectionFactory;
import org.apache.samza.system.eventhub.consumer.EventHubSystemConsumer;
import org.apache.samza.system.eventhub.producer.EventHubSystemProducer;

import java.lang.reflect.Constructor;

public class EventHubSystemFactory implements SystemFactory {

  @SuppressWarnings("unchecked")
  public static SerdeFactory<byte[]> getSerdeFactory(String serdeFactoryClassName) {
    SerdeFactory<byte[]> factory;
    try {
      Class<SerdeFactory<byte[]>> classObj = (Class<SerdeFactory<byte[]>>) Class.forName(serdeFactoryClassName);
      Constructor<SerdeFactory<byte[]>> ctor = classObj.getDeclaredConstructor();
      factory = ctor.newInstance();
    } catch (Exception e) {
      throw new SamzaException("Failed to create Serde Factory for: " + serdeFactoryClassName, e);
    }
    return factory;
  }

  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    return new EventHubSystemConsumer(new EventHubConfig(config, systemName),
            new EventHubEntityConnectionFactory(), registry);
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    return new EventHubSystemProducer(systemName, new EventHubConfig(config, systemName), registry);
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new EventHubSystemAdmin(systemName, new EventHubConfig(config, systemName));
  }
}
