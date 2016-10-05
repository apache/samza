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

package org.apache.samza.system.hdfs;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


/**
 * Util class for methods around partition descriptor.
 *
 * Partition descriptor is rich information about a partition: the set
 * of files that are associated with the partition.
 *
 * Partition descriptor map, or descriptor map, is the map between the
 * {@link org.apache.samza.Partition} and the descriptor
 */
public class PartitionDescriptorUtil {

  private PartitionDescriptorUtil() {

  }

  private static final int INDENT_FACTOR = 2;
  private static final String DELIMITER = ",";

  private static String getStringFromPaths(List<String> paths) {
    return String.join(DELIMITER, paths);
  }

  private static List<String> getPathsFromString(String descriptor) {
    return Arrays.asList(descriptor.split(DELIMITER));
  }

  public static String getJsonFromDescriptorMap(Map<Partition, List<String>> descriptorMap) {
    JSONObject out = new JSONObject();
    descriptorMap.forEach((partition, paths) -> {
      String descriptorStr = getStringFromPaths(paths);
      try {
        out.put(String.valueOf(partition.getPartitionId()), descriptorStr);
      } catch (JSONException e) {
        throw new SamzaException(
          String.format("Invalid description to encode. partition=%s, descriptor=%s", partition, descriptorStr), e);
      }
    });
    try {
      return out.toString(INDENT_FACTOR);
    } catch (JSONException e) {
      throw new SamzaException("Failed to generate json string.", e);
    }
  }

  public static Map<Partition, List<String>> getDescriptorMapFromJson(String json) {
    try {
      @SuppressWarnings("unchecked")
      Map<String, String> rawMap = new ObjectMapper().readValue(json, HashMap.class);
      Map<Partition, List<String>> descriptorMap = new HashMap<>();
      rawMap.forEach((key, value) -> descriptorMap.put(new Partition(Integer.valueOf(key)), getPathsFromString(value)));
      return descriptorMap;
    } catch (IOException | NumberFormatException e) {
      throw new SamzaException("Failed to convert json: " + json, e);
    }
  }

  public static Path getPartitionDescriptorPath(String base, String streamName) {
    Path basePath = new Path(base);
    Path relativePath = new Path(streamName.replaceAll("\\W", "_") + "_partition_description");
    return new Path(basePath, relativePath);
  }
}
