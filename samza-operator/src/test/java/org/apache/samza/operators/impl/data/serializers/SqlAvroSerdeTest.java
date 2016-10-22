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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.serializers.Serde;
import org.apache.samza.operators.impl.data.avro.AvroData;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SqlAvroSerdeTest {
  public static final String ORDER_SCHEMA = "{\"namespace\": \"org.apache.samza.operators\",\n"+
      " \"type\": \"record\",\n"+
      " \"name\": \"Order\",\n"+
      " \"fields\": [\n"+
      "     {\"name\": \"id\", \"type\": \"int\"},\n"+
      "     {\"name\": \"product\",  \"type\": \"string\"},\n"+
      "     {\"name\": \"quantity\", \"type\": \"int\"}\n"+
      " ]\n"+
      "}";

  public static Schema orderSchema = Schema.parse(ORDER_SCHEMA);

  private static Serde serde = new SqlAvroSerdeFactory().getSerde("sqlAvro", sqlAvroSerdeTestConfig());

  @Test
  public void testSqlAvroSerdeDeserialization() throws IOException {
    AvroData decodedDatum = (AvroData)serde.fromBytes(encodeMessage(sampleOrderRecord(), orderSchema));

    Assert.assertTrue(decodedDatum.schema().getType() == org.apache.samza.operators.api.data.Schema.Type.STRUCT);
    Assert.assertTrue(decodedDatum.getFieldData("id").schema().getType() == org.apache.samza.operators.api.data.Schema.Type.INTEGER);
    Assert.assertTrue(decodedDatum.getFieldData("quantity").schema().getType() == org.apache.samza.operators.api.data.Schema.Type.INTEGER);
    Assert.assertTrue(decodedDatum.getFieldData("product").schema().getType() == org.apache.samza.operators.api.data.Schema.Type.STRING);
  }

  @Test
  public void testSqlAvroSerialization() throws IOException {
    AvroData decodedDatumOriginal = (AvroData)serde.fromBytes(encodeMessage(sampleOrderRecord(), orderSchema));
    @SuppressWarnings("unchecked")
    byte[] encodedDatum = serde.toBytes(decodedDatumOriginal);

    AvroData decodedDatum = (AvroData)serde.fromBytes(encodedDatum);

    Assert.assertTrue(decodedDatum.schema().getType() == org.apache.samza.operators.api.data.Schema.Type.STRUCT);
    Assert.assertTrue(decodedDatum.getFieldData("id").schema().getType() == org.apache.samza.operators.api.data.Schema.Type.INTEGER);
    Assert.assertTrue(decodedDatum.getFieldData("quantity").schema().getType() == org.apache.samza.operators.api.data.Schema.Type.INTEGER);
    Assert.assertTrue(decodedDatum.getFieldData("product").schema().getType() == org.apache.samza.operators.api.data.Schema.Type.STRING);
  }

  private static Config sqlAvroSerdeTestConfig(){
    Map<String, String> config = new HashMap<String, String>();
    config.put("serializers.sqlAvro.schema", ORDER_SCHEMA);

    return new MapConfig(config);
  }

  private static byte[] encodeMessage(GenericRecord datum, Schema avroSchema) throws IOException {
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(avroSchema);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    BinaryEncoder encoder = new BinaryEncoder(output);
    writer.write(datum, encoder);
    encoder.flush();

    return  output.toByteArray();
  }

  private static GenericRecord sampleOrderRecord(){
    GenericData.Record datum = new GenericData.Record(orderSchema);
    datum.put("id", 1);
    datum.put("product", "paint");
    datum.put("quantity", 3);

    return datum;
  }
}
