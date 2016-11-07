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
package org.apache.samza.operators.impl.data.serializers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.samza.SamzaException;
import org.apache.samza.serializers.Serde;
import org.apache.samza.operators.impl.data.avro.AvroData;
import org.apache.samza.operators.impl.data.avro.AvroSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class SqlAvroSerde implements Serde<AvroData> {
  private static Logger log = LoggerFactory.getLogger(SqlAvroSerde.class);

  private final Schema avroSchema;
  private final GenericDatumReader<GenericRecord> reader;
  private final GenericDatumWriter<Object> writer;

  public SqlAvroSerde(Schema avroSchema) {
    this.avroSchema = avroSchema;
    this.reader = new GenericDatumReader<GenericRecord>(avroSchema);
    this.writer = new GenericDatumWriter<Object>(avroSchema);
  }

  @Override
  public AvroData fromBytes(byte[] bytes) {
    GenericRecord data;

    try {
      data = reader.read(null, DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null));
      return getAvroData(data, avroSchema);
    } catch (IOException e) {
      String errMsg = "Cannot decode message.";
      log.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  @Override
  public byte[] toBytes(AvroData object) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = new BinaryEncoder(out);

    try {
      writer.write(object.value(), encoder);
      encoder.flush();
      return out.toByteArray();
    } catch (IOException e) {
      String errMsg = "Cannot perform Avro binary encode.";
      log.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  private AvroData getAvroData(GenericRecord data, Schema type){
    AvroSchema schema = AvroSchema.getSchema(type);
    switch (type.getType()){
      case RECORD:
        return AvroData.getStruct(schema, data);
      case ARRAY:
        return AvroData.getArray(schema, data);
      case MAP:
        return AvroData.getMap(schema, data);
      case INT:
        return AvroData.getInt(schema, data);
      case LONG:
        return AvroData.getLong(schema, data);
      case BOOLEAN:
        return AvroData.getBoolean(schema, data);
      case FLOAT:
        return AvroData.getFloat(schema, data);
      case DOUBLE:
        return AvroData.getDouble(schema, data);
      case STRING:
        return AvroData.getString(schema, data);
      case BYTES:
        return AvroData.getBytes(schema, data);
      default:
        throw new IllegalArgumentException("Avro schema: " + type + " is not supported");
    }
  }

  }
