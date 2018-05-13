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
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.SerializerConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OperatorSpecGraphBuilder;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.impl.store.TimestampedValueSerde;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerializableSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.StreamSpec;
import org.junit.Test;

import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestJobNode {

  @Test
  public void testAddSerdeConfigs() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    StreamSpec input1Spec = new StreamSpec("input1", "input1", "input-system");
    StreamSpec input2Spec = new StreamSpec("input2", "input2", "input-system");
    StreamSpec outputSpec = new StreamSpec("output", "output", "output-system");
    StreamSpec partitionBySpec =
        new StreamSpec("jobName-jobId-partition_by-p1", "partition_by-p1", "intermediate-system");
    doReturn(input1Spec).when(mockRunner).getStreamSpec("input1");
    doReturn(input2Spec).when(mockRunner).getStreamSpec("input2");
    doReturn(outputSpec).when(mockRunner).getStreamSpec("output");
    doReturn(partitionBySpec).when(mockRunner).getStreamSpec("jobName-jobId-partition_by-p1");

    Config mockConfig = mock(Config.class);
    when(mockConfig.get(JobConfig.JOB_NAME())).thenReturn("jobName");
    when(mockConfig.get(eq(JobConfig.JOB_ID()), anyString())).thenReturn("jobId");

    OperatorSpecGraphBuilder graphBuilder = new OperatorSpecGraphBuilder(mockRunner, mockConfig);
    graphBuilder.setDefaultSerde(KVSerde.of(new StringSerde(), new JsonSerdeV2<>()));
    MessageStream<KV<String, Object>> input1 = graphBuilder.getInputStream("input1");
    MessageStream<KV<String, Object>> input2 = graphBuilder.getInputStream("input2");
    OutputStream<KV<String, Object>> output = graphBuilder.getOutputStream("output");
    JoinFunction<String, Object, Object, KV<String, Object>> mockJoinFn = mock(JoinFunction.class);
    input1
        .partitionBy(KV::getKey, KV::getValue, "p1").map(kv -> kv.value)
        .join(input2.map(kv -> kv.value), mockJoinFn,
            new StringSerde(), new JsonSerdeV2<>(Object.class), new JsonSerdeV2<>(Object.class),
            Duration.ofHours(1), "j1")
        .sendTo(output);

    JobNode jobNode = new JobNode("jobName", "jobId", graphBuilder.build(), mockConfig);
    Config config = new MapConfig();
    StreamEdge input1Edge = new StreamEdge(input1Spec, config);
    StreamEdge input2Edge = new StreamEdge(input2Spec, config);
    StreamEdge outputEdge = new StreamEdge(outputSpec, config);
    StreamEdge repartitionEdge = new StreamEdge(partitionBySpec, true, config);
    jobNode.addInEdge(input1Edge);
    jobNode.addInEdge(input2Edge);
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
    assertEquals(5, serializers.size()); // 2 default + 3 specific for join

    String input1KeySerde = mapConfig.get("streams.input1.samza.key.serde");
    String input1MsgSerde = mapConfig.get("streams.input1.samza.msg.serde");
    assertTrue("Serialized serdes should contain input1 key serde",
        deserializedSerdes.containsKey(input1KeySerde));
    assertTrue("Serialized input1 key serde should be a StringSerde",
        input1KeySerde.startsWith(StringSerde.class.getSimpleName()));
    assertTrue("Serialized serdes should contain input1 msg serde",
        deserializedSerdes.containsKey(input1MsgSerde));
    assertTrue("Serialized input1 msg serde should be a JsonSerdeV2",
        input1MsgSerde.startsWith(JsonSerdeV2.class.getSimpleName()));

    String input2KeySerde = mapConfig.get("streams.input2.samza.key.serde");
    String input2MsgSerde = mapConfig.get("streams.input2.samza.msg.serde");
    assertTrue("Serialized serdes should contain input2 key serde",
        deserializedSerdes.containsKey(input2KeySerde));
    assertTrue("Serialized input2 key serde should be a StringSerde",
        input2KeySerde.startsWith(StringSerde.class.getSimpleName()));
    assertTrue("Serialized serdes should contain input2 msg serde",
        deserializedSerdes.containsKey(input2MsgSerde));
    assertTrue("Serialized input2 msg serde should be a JsonSerdeV2",
        input2MsgSerde.startsWith(JsonSerdeV2.class.getSimpleName()));

    String outputKeySerde = mapConfig.get("streams.output.samza.key.serde");
    String outputMsgSerde = mapConfig.get("streams.output.samza.msg.serde");
    assertTrue("Serialized serdes should contain output key serde",
        deserializedSerdes.containsKey(outputKeySerde));
    assertTrue("Serialized output key serde should be a StringSerde",
        outputKeySerde.startsWith(StringSerde.class.getSimpleName()));
    assertTrue("Serialized serdes should contain output msg serde",
        deserializedSerdes.containsKey(outputMsgSerde));
    assertTrue("Serialized output msg serde should be a StringSerde",
        outputMsgSerde.startsWith(JsonSerdeV2.class.getSimpleName()));

    String partitionByKeySerde = mapConfig.get("streams.jobName-jobId-partition_by-p1.samza.key.serde");
    String partitionByMsgSerde = mapConfig.get("streams.jobName-jobId-partition_by-p1.samza.msg.serde");
    assertTrue("Serialized serdes should contain intermediate stream key serde",
        deserializedSerdes.containsKey(partitionByKeySerde));
    assertTrue("Serialized intermediate stream key serde should be a StringSerde",
        partitionByKeySerde.startsWith(StringSerde.class.getSimpleName()));
    assertTrue("Serialized serdes should contain intermediate stream msg serde",
        deserializedSerdes.containsKey(partitionByMsgSerde));
    assertTrue(
        "Serialized intermediate stream msg serde should be a StringSerde",
        partitionByMsgSerde.startsWith(JsonSerdeV2.class.getSimpleName()));

    String leftJoinStoreKeySerde = mapConfig.get("stores.jobName-jobId-join-j1-L.key.serde");
    String leftJoinStoreMsgSerde = mapConfig.get("stores.jobName-jobId-join-j1-L.msg.serde");
    assertTrue("Serialized serdes should contain left join store key serde",
        deserializedSerdes.containsKey(leftJoinStoreKeySerde));
    assertTrue("Serialized left join store key serde should be a StringSerde",
        leftJoinStoreKeySerde.startsWith(StringSerde.class.getSimpleName()));
    assertTrue("Serialized serdes should contain left join store msg serde",
        deserializedSerdes.containsKey(leftJoinStoreMsgSerde));
    assertTrue("Serialized left join store msg serde should be a TimestampedValueSerde",
        leftJoinStoreMsgSerde.startsWith(TimestampedValueSerde.class.getSimpleName()));

    String rightJoinStoreKeySerde = mapConfig.get("stores.jobName-jobId-join-j1-R.key.serde");
    String rightJoinStoreMsgSerde = mapConfig.get("stores.jobName-jobId-join-j1-R.msg.serde");
    assertTrue("Serialized serdes should contain right join store key serde",
        deserializedSerdes.containsKey(rightJoinStoreKeySerde));
    assertTrue("Serialized right join store key serde should be a StringSerde",
        rightJoinStoreKeySerde.startsWith(StringSerde.class.getSimpleName()));
    assertTrue("Serialized serdes should contain right join store msg serde",
        deserializedSerdes.containsKey(rightJoinStoreMsgSerde));
    assertTrue("Serialized right join store msg serde should be a TimestampedValueSerde",
        rightJoinStoreMsgSerde.startsWith(TimestampedValueSerde.class.getSimpleName()));
  }

}
