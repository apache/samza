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

package org.apache.samza.execution;

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.SerializerConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerializableSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.StreamSpec;
import org.junit.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestJobNode {

  @Test
  public void testAddSerdeConfigs() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    StreamSpec inputSpec = new StreamSpec("input", "input", "input-system");
    StreamSpec outputSpec = new StreamSpec("output", "output", "output-system");
    StreamSpec partitionBySpec = new StreamSpec("null-null-partition_by-1", "partition_by-1", "intermediate-system");
    doReturn(inputSpec).when(mockRunner).getStreamSpec("input");
    doReturn(outputSpec).when(mockRunner).getStreamSpec("output");
    doReturn(partitionBySpec).when(mockRunner).getStreamSpec("null-null-partition_by-1");

    StreamGraphImpl streamGraph = new StreamGraphImpl(mockRunner, mock(Config.class));
    streamGraph.setDefaultSerde(KVSerde.of(new StringSerde(), new JsonSerdeV2<>()));
    MessageStream<KV<String, Object>> input = streamGraph.getInputStream("input");
    OutputStream<KV<String, Object>> output = streamGraph.getOutputStream("output");
    input.partitionBy(KV::getKey, KV::getValue).sendTo(output);

    JobNode jobNode = new JobNode("jobName", "jobId", streamGraph, mock(Config.class));
    Config config = new MapConfig();
    StreamEdge inputEdge = new StreamEdge(inputSpec, config);
    StreamEdge outputEdge = new StreamEdge(outputSpec, config);
    StreamEdge repartitionEdge = new StreamEdge(partitionBySpec, true, config);
    jobNode.addInEdge(inputEdge);
    jobNode.addOutEdge(outputEdge);
    jobNode.addInEdge(repartitionEdge);
    jobNode.addOutEdge(repartitionEdge);

    Map<String, String> configs = new HashMap<>();
    jobNode.addSerdeConfigs(configs);

    MapConfig mapConfig = new MapConfig(configs);
    Config serializers = mapConfig.subset("serializers.registry.", true);

    // make sure that the serializers deserialize correctly
    SerializableSerde<Serde> serializableSerde = new SerializableSerde<>();
    Map<String, Serde> deserializedSerdes = serializers.entrySet().stream().collect(Collectors.toMap(
        e -> e.getKey().replace(SerializerConfig.SERIALIZED_INSTANCE_SUFFIX(), ""),
        e -> serializableSerde.fromBytes(Base64.getDecoder().decode(e.getValue().getBytes()))
    ));
    assertEquals(2, serializers.size());

    String inputKeySerde = mapConfig.get("streams.input.samza.key.serde");
    String inputMsgSerde = mapConfig.get("streams.input.samza.msg.serde");
    assertTrue(deserializedSerdes.containsKey(inputKeySerde));
    assertTrue(inputKeySerde.startsWith(StringSerde.class.getSimpleName()));
    assertTrue(deserializedSerdes.containsKey(inputMsgSerde));
    assertTrue(inputMsgSerde.startsWith(JsonSerdeV2.class.getSimpleName()));

    String outputKeySerde = mapConfig.get("streams.output.samza.key.serde");
    String outputMsgSerde = mapConfig.get("streams.output.samza.msg.serde");
    assertTrue(deserializedSerdes.containsKey(outputKeySerde));
    assertTrue(outputKeySerde.startsWith(StringSerde.class.getSimpleName()));
    assertTrue(deserializedSerdes.containsKey(outputMsgSerde));
    assertTrue(outputMsgSerde.startsWith(JsonSerdeV2.class.getSimpleName()));

    String partitionByKeySerde = mapConfig.get("streams.null-null-partition_by-1.samza.key.serde");
    String partitionByMsgSerde = mapConfig.get("streams.null-null-partition_by-1.samza.msg.serde");
    assertTrue(deserializedSerdes.containsKey(partitionByKeySerde));
    assertTrue(partitionByKeySerde.startsWith(StringSerde.class.getSimpleName()));
    assertTrue(deserializedSerdes.containsKey(partitionByMsgSerde));
    assertTrue(partitionByMsgSerde.startsWith(JsonSerdeV2.class.getSimpleName()));
  }

}
