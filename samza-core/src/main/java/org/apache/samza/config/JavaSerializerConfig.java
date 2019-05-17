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

package org.apache.samza.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.samza.SamzaException;
import org.apache.samza.serializers.ByteBufferSerdeFactory;
import org.apache.samza.serializers.ByteSerdeFactory;
import org.apache.samza.serializers.DoubleSerdeFactory;
import org.apache.samza.serializers.IntegerSerdeFactory;
import org.apache.samza.serializers.JsonSerdeFactory;
import org.apache.samza.serializers.LongSerdeFactory;
import org.apache.samza.serializers.SerializableSerdeFactory;
import org.apache.samza.serializers.StringSerdeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provides helpers for accessing configs related to serialization and deserialization.
 */
public class JavaSerializerConfig extends MapConfig {
  public static final Logger LOGGER = LoggerFactory.getLogger(JavaSerializerConfig.class);

  public static final String SERIALIZER_PREFIX = "serializers.registry.%s";
  public static final String SERDE_FACTORY_CLASS = SERIALIZER_PREFIX + ".class";
  public static final String SERIALIZED_INSTANCE_SUFFIX = ".samza.serialized.instance";
  public static final String SERDE_SERIALIZED_INSTANCE = SERIALIZER_PREFIX + SERIALIZED_INSTANCE_SUFFIX;

  public JavaSerializerConfig(Config config) {
    super(config);
  }

  /**
   * Returns the pre-defined serde factory class name for the provided serde name. If no pre-defined factory exists,
   * throws an exception.
   */
  public static String getSerdeFactoryName(String serdeName) {
    String serdeFactoryName = doGetSerdeFactoryName(serdeName);
    LOGGER.info(String.format("Using default serde %s for serde name %s", serdeFactoryName, serdeName));
    return serdeFactoryName;
  }

  /**
   * @param name name of the serde in the config
   * @return serde factory class name for {@code name}; empty if no class was found in the config
   */
  public Optional<String> getSerdeClass(String name) {
    return Optional.ofNullable(get(String.format(SERDE_FACTORY_CLASS, name), null));
  }

  /**
   * Useful for getting individual serializers.
   * @return a list of all serializer names from the config file
   */
  public List<String> getSerdeNames() {
    List<String> results = new ArrayList<String>();
    Config subConfig = subset(String.format(SERIALIZER_PREFIX, ""), true);
    for (String key : subConfig.keySet()) {
      if (key.endsWith(".class")) {
        results.add(key.replace(".class", ""));
      }
    }
    return results;
  }

  private static String doGetSerdeFactoryName(String serdeName) {
    switch (serdeName) {
      case "byte":
        return ByteSerdeFactory.class.getCanonicalName();
      case "bytebuffer":
        return ByteBufferSerdeFactory.class.getCanonicalName();
      case "integer":
        return IntegerSerdeFactory.class.getCanonicalName();
      case "json":
        return JsonSerdeFactory.class.getCanonicalName();
      case "long":
        return LongSerdeFactory.class.getCanonicalName();
      case "serializable":
        return SerializableSerdeFactory.class.getCanonicalName();
      case "string":
        return StringSerdeFactory.class.getCanonicalName();
      case "double":
        return DoubleSerdeFactory.class.getCanonicalName();
      default:
        throw new SamzaException(String.format("No pre-defined factory class name for serde name %s", serdeName));
    }
  }
}
