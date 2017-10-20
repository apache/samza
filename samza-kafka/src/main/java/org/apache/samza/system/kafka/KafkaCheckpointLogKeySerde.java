package org.apache.samza.system.kafka;

import org.apache.samza.SamzaException;
import org.apache.samza.container.TaskName;
import org.apache.samza.serializers.Serde;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.module.SimpleModule;

import java.io.IOException;

/**
 * A serde for {@link KafkaCheckpointLogKey}
 */
public class KafkaCheckpointLogKeySerde implements Serde<KafkaCheckpointLogKey> {

  private static final String SSP_GROUPER_FACTORY_FIELD = "systemstreampartition-grouper-factory";
  private static final String TASK_NAME_FIELD = "taskName";
  private static final String TYPE_FIELD = "type";

  private final ObjectMapper mapper;

  public KafkaCheckpointLogKeySerde() {
    mapper = new ObjectMapper();

    SimpleModule module = new SimpleModule("SamzaCheckpoint", new Version(1, 0, 0, ""));
    module.addSerializer(KafkaCheckpointLogKey.class, new KafkaCheckpointLogKeySerializer());
    module.addDeserializer(KafkaCheckpointLogKey.class, new KafkaCheckpointLogKeyDeserializer());
    mapper.registerModule(module);
  }

  @Override
  public byte[] toBytes(KafkaCheckpointLogKey key) {
    try {
      return new String(mapper.writeValueAsString(key)).getBytes();
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public KafkaCheckpointLogKey fromBytes(byte[] bytes) {
    try {
      return mapper.readValue(bytes, KafkaCheckpointLogKey.class);
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  private static class KafkaCheckpointLogKeySerializer extends JsonSerializer<KafkaCheckpointLogKey> {
    @Override
    public void serialize(KafkaCheckpointLogKey value, JsonGenerator generator, SerializerProvider provider)
        throws IOException {

      generator.writeStartObject();
      generator.writeStringField(SSP_GROUPER_FACTORY_FIELD, value.getGrouperFactoryClassName());
      generator.writeStringField(TASK_NAME_FIELD, value.getTaskName().toString());
      generator.writeStringField(TYPE_FIELD, value.getType());
      generator.writeEndObject();
    }
  }

  private static class KafkaCheckpointLogKeyDeserializer extends JsonDeserializer<KafkaCheckpointLogKey> {
    @Override
    public KafkaCheckpointLogKey deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
      JsonNode node = jp.getCodec().readTree(jp);
      String grouperClass = node.get(SSP_GROUPER_FACTORY_FIELD).asText();
      TaskName taskName = new TaskName(node.get(TASK_NAME_FIELD).asText());
      String type = node.get(TYPE_FIELD).asText();

      return new KafkaCheckpointLogKey(grouperClass, taskName, type);
    }
  }

}
