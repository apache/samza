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

package org.apache.samza.tools.json;

import java.io.IOException;
import java.util.List;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.sql.SamzaSqlRelRecord;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.RelSchemaProvider;
import org.apache.samza.sql.interfaces.SamzaRelConverter;
import org.apache.samza.sql.interfaces.SamzaRelConverterFactory;
import org.apache.samza.system.SystemStream;


/**
 * SamzaRelConverter that can convert {@link SamzaSqlRelMessage} to json string byte array.
 */
public class JsonRelConverterFactory implements SamzaRelConverterFactory {

  ObjectMapper mapper = new ObjectMapper();

  @Override
  public SamzaRelConverter create(SystemStream systemStream, RelSchemaProvider relSchemaProvider, Config config) {
    return new JsonRelConverter();
  }

  public class JsonRelConverter implements SamzaRelConverter {

    @Override
    public SamzaSqlRelMessage convertToRelMessage(KV<Object, Object> kv) {
      throw new NotImplementedException("Not implemented.");
    }

    @Override
    public KV<Object, Object> convertToSamzaMessage(SamzaSqlRelMessage relMessage) {
      String jsonValue = convertToSamzaMessage(relMessage.getSamzaSqlRelRecord());
      return new KV<>(relMessage.getKey(), jsonValue.getBytes());
    }

    private String convertToSamzaMessage(SamzaSqlRelRecord relRecord) {
      String jsonValue;
      ObjectNode node = mapper.createObjectNode();

      List<String> fieldNames = relRecord.getFieldNames();
      List<Object> values = relRecord.getFieldValues();

      for (int index = 0; index < fieldNames.size(); index++) {
        Object value = values.get(index);
        if (value == null) {
          continue;
        }

        // TODO limited support right now.
        if (Long.class.isAssignableFrom(value.getClass())) {
          node.put(fieldNames.get(index), (Long) value);
        } else if (Integer.class.isAssignableFrom(value.getClass())) {
          node.put(fieldNames.get(index), (Integer) value);
        } else if (Double.class.isAssignableFrom(value.getClass())) {
          node.put(fieldNames.get(index), (Double) value);
        } else if (String.class.isAssignableFrom(value.getClass())) {
          node.put(fieldNames.get(index), (String) value);
        } else if (SamzaSqlRelRecord.class.isAssignableFrom(value.getClass())) {
          // If the value is a SamzaSqlRelRecord, call convertToSamzaMessage to convert the record to json string.
          node.put(fieldNames.get(index), convertToSamzaMessage((SamzaSqlRelRecord) value));
        } else {
          node.put(fieldNames.get(index), value.toString());
        }
      }
      try {
        jsonValue = mapper.writeValueAsString(node);
      } catch (IOException e) {
        throw new SamzaException("Error json serializing object", e);
      }

      return jsonValue;
    }
  }
}
