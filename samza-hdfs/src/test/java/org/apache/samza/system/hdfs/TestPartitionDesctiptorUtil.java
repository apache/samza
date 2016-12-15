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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.junit.Assert;
import org.junit.Test;


public class TestPartitionDesctiptorUtil {

  @Test
  public void testBasicEncodeDecode() {
    Map<Partition, List<String>> input = new HashMap<>();
    input.put(new Partition(0), Collections.singletonList("path_0"));
    String[] array = {"path_1", "path_2"};
    input.put(new Partition(1), Arrays.asList(array));
    input.put(new Partition(3), Collections.singletonList("path_3"));
    String json = PartitionDescriptorUtil.getJsonFromDescriptorMap(input);
    Map<Partition, List<String>> output = PartitionDescriptorUtil.getDescriptorMapFromJson(json);
    Assert.assertEquals(3, output.entrySet().size());
    Assert.assertTrue(output.containsKey(new Partition(0)));
    Assert.assertEquals("path_0", output.get(new Partition(0)).get(0));
    Assert.assertTrue(output.containsKey(new Partition(1)));
    Assert.assertEquals("path_1", output.get(new Partition(1)).get(0));
    Assert.assertEquals("path_2", output.get(new Partition(1)).get(1));
    Assert.assertTrue(output.containsKey(new Partition(3)));
    Assert.assertEquals("path_3", output.get(new Partition(3)).get(0));
  }

  @Test
  public void testSingleEntry() {
    Map<Partition, List<String>> input = new HashMap<>();
    input.put(new Partition(1), Collections.singletonList("random_path_1"));
    String json = PartitionDescriptorUtil.getJsonFromDescriptorMap(input);
    Map<Partition, List<String>> output = PartitionDescriptorUtil.getDescriptorMapFromJson(json);
    Assert.assertEquals(1, output.entrySet().size());
    Assert.assertTrue(output.containsKey(new Partition(1)));
    Assert.assertEquals("random_path_1", output.get(new Partition(1)).get(0));
  }

  @Test
  public void testKeyOverriding() {
    Map<Partition, List<String>> input = new HashMap<>();
    input.put(new Partition(0), Collections.singletonList("path_0"));
    input.put(new Partition(0), Collections.singletonList("new_path_0"));
    String json = PartitionDescriptorUtil.getJsonFromDescriptorMap(input);
    Map<Partition, List<String>> output = PartitionDescriptorUtil.getDescriptorMapFromJson(json);
    Assert.assertEquals(1, output.entrySet().size());
    Assert.assertTrue(output.containsKey(new Partition(0)));
    Assert.assertEquals("new_path_0", output.get(new Partition(0)).get(0));
  }

  @Test
  public void testEmptyInput() {
    Map<Partition, List<String>> input = new HashMap<>();
    String json = PartitionDescriptorUtil.getJsonFromDescriptorMap(input);
    Assert.assertNotNull(json);
    Map<Partition, List<String>> output = PartitionDescriptorUtil.getDescriptorMapFromJson(json);
    Assert.assertTrue(output.isEmpty());
  }

  @Test(expected = SamzaException.class)
  public void testInvalidInput() {
    String json = "invalidStr";
    PartitionDescriptorUtil.getDescriptorMapFromJson(json);
  }
}
