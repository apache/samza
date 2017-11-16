/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.samza.sql.avro;

import com.google.common.base.Joiner;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.sql.avro.schemas.ComplexRecord;
import org.apache.samza.sql.avro.schemas.SimpleRecord;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.system.SystemStream;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestAvroRelConversion {

  private static final Logger LOG = LoggerFactory.getLogger(TestAvroRelConversion.class);
  private final AvroRelConverter simpleRecordAvroRelConverter;
  private final AvroRelConverter complexRecordAvroRelConverter;
  private final AvroRelSchemaProvider simpleRecordSchemaProvider;
  private final AvroRelSchemaProvider complexRecordSchemProvider;

  private int id = 1;
  private boolean boolValue = true;
  private double doubleValue = 0.6;
  private float floatValue = 0.6f;
  private String testStrValue = "testString";
  private ByteBuffer testBytes = ByteBuffer.wrap("testBytes".getBytes());
  private long longValue = 200L;

  private HashMap<String, String> mapValue = new HashMap<String, String>() {{
    put("key1", "val1");
    put("key2", "val2");
    put("key3", "val3");
  }};
  private List<String> arrayValue = Arrays.asList("val1", "val2", "val3");

  public TestAvroRelConversion() {
    Map<String, String> props = new HashMap<>();
    SystemStream ss1 = new SystemStream("test", "complexRecord");
    SystemStream ss2 = new SystemStream("test", "simpleRecord");
    props.put(
        String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA, ss1.getSystem(), ss1.getStream()),
        ComplexRecord.SCHEMA$.toString());
    props.put(
        String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA, ss2.getSystem(), ss2.getStream()),
        SimpleRecord.SCHEMA$.toString());

    ConfigBasedAvroRelSchemaProviderFactory factory = new ConfigBasedAvroRelSchemaProviderFactory();

    complexRecordSchemProvider = (AvroRelSchemaProvider) factory.create(ss1, new MapConfig(props));
    simpleRecordSchemaProvider = (AvroRelSchemaProvider) factory.create(ss2, new MapConfig(props));
    complexRecordAvroRelConverter = new AvroRelConverter(ss1, complexRecordSchemProvider, new MapConfig());
    simpleRecordAvroRelConverter = new AvroRelConverter(ss2, simpleRecordSchemaProvider, new MapConfig());
  }

  @Test
  public void testSimpleSchemaConversion() {
    String streamName = "stream";

    RelDataType dataType = simpleRecordSchemaProvider.getRelationalSchema();
    junit.framework.Assert.assertTrue(dataType instanceof RelRecordType);
    RelRecordType recordType = (RelRecordType) dataType;

    junit.framework.Assert.assertEquals(recordType.getFieldCount(), SimpleRecord.SCHEMA$.getFields().size());
    junit.framework.Assert.assertTrue(
        recordType.getField("id", true, false).getType().getSqlTypeName() == SqlTypeName.INTEGER);
    junit.framework.Assert.assertTrue(
        recordType.getField("name", true, false).getType().getSqlTypeName() == SqlTypeName.VARCHAR);

    LOG.info("Relational schema " + dataType);
  }

  @Test
  public void testComplexSchemaConversion() {
    RelDataType relSchema = complexRecordSchemProvider.getRelationalSchema();

    LOG.info("Relational schema " + relSchema);
  }

  @Test
  public void testSimpleRecordConversion() {

    GenericData.Record record = new GenericData.Record(SimpleRecord.SCHEMA$);
    record.put("id", 1);
    record.put("name", "name1");

    SamzaSqlRelMessage message = simpleRecordAvroRelConverter.convertToRelMessage(new KV<>("key", record));
    LOG.info(Joiner.on(",").join(message.getFieldValues()));
    LOG.info(Joiner.on(",").join(message.getFieldNames()));
  }

  public static <T> byte[] encodeAvroSpecificRecord(Class<T> clazz, T record) throws IOException {
    DatumWriter<T> msgDatumWriter = new SpecificDatumWriter<>(clazz);
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    Encoder encoder = EncoderFactory.get().binaryEncoder(os, null);
    msgDatumWriter.write(record, encoder);
    encoder.flush();
    return os.toByteArray();
  }

  @Test
  public void testComplexRecordConversion() throws IOException {
    GenericData.Record record = new GenericData.Record(ComplexRecord.SCHEMA$);
    record.put("id", id);
    record.put("bool_value", boolValue);
    record.put("double_value", doubleValue);
    record.put("float_value", floatValue);
    record.put("string_value", testStrValue);
    record.put("bytes_value", testBytes);
    record.put("long_value", longValue);
    record.put("array_values", arrayValue);
    record.put("map_values", mapValue);

    ComplexRecord complexRecord = new ComplexRecord();
    complexRecord.id = id;
    complexRecord.bool_value = boolValue;
    complexRecord.double_value = doubleValue;
    complexRecord.float_value = floatValue;
    complexRecord.string_value = testStrValue;
    complexRecord.bytes_value = testBytes;
    complexRecord.long_value = longValue;
    complexRecord.array_values = new ArrayList<>();
    complexRecord.array_values.addAll(arrayValue);
    complexRecord.map_values = new HashMap<>();
    complexRecord.map_values.putAll(mapValue);

    byte[] serializedData = bytesFromGenericRecord(record);
    validateAvroSerializedData(serializedData);

    serializedData = encodeAvroSpecificRecord(ComplexRecord.class, complexRecord);
    validateAvroSerializedData(serializedData);
  }

  private static <T> T genericRecordFromBytes(byte[] bytes, Schema schema) throws IOException {
    BinaryDecoder binDecoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
    GenericDatumReader<T> reader = new GenericDatumReader<>(schema);
    return reader.read(null, binDecoder);
  }

  private static byte[] bytesFromGenericRecord(GenericRecord record) throws IOException {
    DatumWriter<IndexedRecord> datumWriter;
    datumWriter = new GenericDatumWriter<>(record.getSchema());
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    datumWriter.write(record, encoder);
    encoder.flush();
    outputStream.close();
    return outputStream.toByteArray();
  }

  private void validateAvroSerializedData(byte[] serializedData) throws IOException {
    GenericRecord complexRecordValue = genericRecordFromBytes(serializedData, ComplexRecord.SCHEMA$);

    String streamName = "stream";
    RelDataType dataType = complexRecordSchemProvider.getRelationalSchema();

    SamzaSqlRelMessage message = complexRecordAvroRelConverter.convertToRelMessage(new KV<>("key", complexRecordValue));
    Assert.assertEquals(message.getFieldNames().size(),
        ComplexRecord.SCHEMA$.getFields().size());

    Assert.assertEquals(message.getField("id").get(), id);
    Assert.assertEquals(message.getField("bool_value").get(), boolValue);
    Assert.assertEquals(message.getField("double_value").get(), doubleValue);
    Assert.assertEquals(message.getField("string_value").get(), new Utf8(testStrValue));
    Assert.assertEquals(message.getField("float_value").get(), floatValue);
    Assert.assertEquals(message.getField("long_value").get(), longValue);
    Assert.assertTrue(
        arrayValue.stream().map(Utf8::new).collect(Collectors.toList()).equals(message.getField("array_values").get()));
    Assert.assertTrue(mapValue.entrySet()
        .stream()
        .collect(Collectors.toMap(x -> new Utf8(x.getKey()), y -> new Utf8(y.getValue())))
        .equals(message.getField("map_values").get()));

    Assert.assertTrue(message.getField("bytes_value").get().equals(testBytes));

    LOG.info(Joiner.on(",").useForNull("null").join(message.getFieldValues()));
    LOG.info(Joiner.on(",").join(message.getFieldNames()));

    KV<Object, Object> samzaMessage = complexRecordAvroRelConverter.convertToSamzaMessage(message);
    GenericRecord record = (GenericRecord) samzaMessage.getValue();

    for (Schema.Field field : ComplexRecord.SCHEMA$.getFields()) {
      if (field.name().equals("array_values")) {
        Assert.assertTrue(record.get(field.name()).equals(complexRecordValue.get(field.name())));
      } else {
        Assert.assertEquals(record.get(field.name()), complexRecordValue.get(field.name()));
      }
    }
  }
}
