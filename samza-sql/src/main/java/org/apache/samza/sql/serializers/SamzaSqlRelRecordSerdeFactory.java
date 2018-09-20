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

package org.apache.samza.sql.serializers;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.sql.SamzaSqlRelRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


/**
 * A serializer for {@link SamzaSqlRelRecord}. This serializer preserves the type information as
 * {@link SamzaSqlRelRecord} and contains nested {@link SamzaSqlRelRecord}
 * records.
 */
public final class SamzaSqlRelRecordSerdeFactory implements SerdeFactory<SamzaSqlRelRecord> {
  public Serde<SamzaSqlRelRecord> getSerde(String name, Config config) {
    return new SamzaSqlRelRecordSerde();
  }

  public final static class SamzaSqlRelRecordSerde implements Serde<SamzaSqlRelRecord> {

    @Override
    public SamzaSqlRelRecord fromBytes(byte[] bytes) {
      try {
        ObjectMapper mapper = new ObjectMapper();
        // Enable object typing to handle nested records
        mapper.enableDefaultTyping();
        return mapper.readValue(new String(bytes, "UTF-8"), new TypeReference<SamzaSqlRelRecord>() {});
      } catch (Exception e) {
        throw new SamzaException(e);
      }
    }

    @Override
    public byte[] toBytes(SamzaSqlRelRecord p) {
      try {
        ObjectMapper mapper = new ObjectMapper();
        // Enable object typing to handle nested records
        mapper.enableDefaultTyping();
        return mapper.writeValueAsString(p).getBytes("UTF-8");
      } catch (Exception e) {
        throw new SamzaException(e);
      }
    }
  }
}
