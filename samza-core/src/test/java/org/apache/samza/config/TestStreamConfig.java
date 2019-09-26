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

import java.util.Collections;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.samza.system.SystemStream;
import org.junit.Test;
import scala.Option;
import scala.collection.JavaConverters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TestStreamConfig {
  private static final String SYSTEM = "system";
  private static final String STREAM_ID = "streamId";
  private static final SystemStream SYSTEM_STREAM = new SystemStream(SYSTEM, STREAM_ID);
  private static final String OTHER_STREAM_ID = "otherStreamId";
  private static final String PHYSICAL_STREAM = "physicalStream";
  private static final SystemStream SYSTEM_STREAM_PHYSICAL = new SystemStream(SYSTEM, PHYSICAL_STREAM);
  private static final String SAMZA_IGNORED_PROPERTY = "samza.ignored.property";
  private static final String UNUSED_VALUE = "should_not_be_used";

  @Test
  public void testGetStreamMsgSerde() {
    String value = "my.msg.serde";
    doTestSamzaProperty(StreamConfigJava.MSG_SERDE, value,
        (config, systemStream) -> assertEquals(Option.apply(value), config.getStreamMsgSerde(systemStream)));
    doTestSamzaProperty(StreamConfigJava.MSG_SERDE, "",
        (config, systemStream) -> assertEquals(Option.empty(), config.getStreamMsgSerde(systemStream)));
    doTestSamzaPropertyDoesNotExist(StreamConfigJava.MSG_SERDE,
        (config, systemStream) -> assertEquals(Option.empty(), config.getStreamMsgSerde(systemStream)));
    doTestSamzaPropertyInvalidConfig(StreamConfigJava::getStreamMsgSerde);
  }

  @Test
  public void testGetStreamKeySerde() {
    String value = "my.key.serde";
    doTestSamzaProperty(StreamConfigJava.KEY_SERDE, value,
        (config, systemStream) -> assertEquals(Option.apply(value), config.getStreamKeySerde(systemStream)));
    doTestSamzaProperty(StreamConfigJava.KEY_SERDE, "",
        (config, systemStream) -> assertEquals(Option.empty(), config.getStreamKeySerde(systemStream)));
    doTestSamzaPropertyDoesNotExist(StreamConfigJava.KEY_SERDE,
        (config, systemStream) -> assertEquals(Option.empty(), config.getStreamKeySerde(systemStream)));
    doTestSamzaPropertyInvalidConfig(StreamConfigJava::getStreamKeySerde);
  }

  @Test
  public void testGetResetOffset() {
    doTestSamzaProperty(StreamConfigJava.CONSUMER_RESET_OFFSET, "true",
        (config, systemStream) -> assertTrue(config.getResetOffset(systemStream)));
    doTestSamzaProperty(StreamConfigJava.CONSUMER_RESET_OFFSET, "false",
        (config, systemStream) -> assertFalse(config.getResetOffset(systemStream)));
    // if not true/false, then use false
    doTestSamzaProperty(StreamConfigJava.CONSUMER_RESET_OFFSET, "unknown_value",
        (config, systemStream) -> assertFalse(config.getResetOffset(systemStream)));
    doTestSamzaPropertyDoesNotExist(StreamConfigJava.CONSUMER_RESET_OFFSET,
        (config, systemStream) -> assertFalse(config.getResetOffset(systemStream)));
    doTestSamzaPropertyInvalidConfig(StreamConfigJava::getResetOffset);
  }

  @Test
  public void testIsResetOffsetConfigured() {
    doTestSamzaProperty(StreamConfigJava.CONSUMER_RESET_OFFSET, "true",
        (config, systemStream) -> assertTrue(config.isResetOffsetConfigured(systemStream)));
    doTestSamzaProperty(StreamConfigJava.CONSUMER_RESET_OFFSET, "false",
        (config, systemStream) -> assertTrue(config.isResetOffsetConfigured(systemStream)));
    // if not true/false, then use false
    doTestSamzaProperty(StreamConfigJava.CONSUMER_RESET_OFFSET, "unknown_value",
        (config, systemStream) -> assertTrue(config.isResetOffsetConfigured(systemStream)));
    doTestSamzaPropertyDoesNotExist(StreamConfigJava.CONSUMER_RESET_OFFSET,
        (config, systemStream) -> assertFalse(config.isResetOffsetConfigured(systemStream)));
    doTestSamzaPropertyInvalidConfig(StreamConfigJava::isResetOffsetConfigured);
  }

  @Test
  public void testGetDefaultStreamOffset() {
    String value = "my_offset_default";
    doTestSamzaProperty(StreamConfigJava.CONSUMER_OFFSET_DEFAULT, value,
        (config, systemStream) -> assertEquals(Option.apply(value), config.getDefaultStreamOffset(systemStream)));
    doTestSamzaProperty(StreamConfigJava.CONSUMER_OFFSET_DEFAULT, "",
        (config, systemStream) -> assertEquals(Option.apply(""), config.getDefaultStreamOffset(systemStream)));
    doTestSamzaPropertyDoesNotExist(StreamConfigJava.CONSUMER_OFFSET_DEFAULT,
        (config, systemStream) -> assertEquals(Option.empty(),
            new StreamConfigJava(config).getDefaultStreamOffset(systemStream)));
    doTestSamzaPropertyInvalidConfig(StreamConfigJava::getDefaultStreamOffset);
  }

  @Test
  public void testIsDefaultStreamOffsetConfigured() {
    String value = "my_offset_default";
    doTestSamzaProperty(StreamConfigJava.CONSUMER_OFFSET_DEFAULT, value,
        (config, systemStream) -> assertTrue(config.isDefaultStreamOffsetConfigured(systemStream)));
    doTestSamzaProperty(StreamConfigJava.CONSUMER_OFFSET_DEFAULT, "",
        (config, systemStream) -> assertTrue(config.isDefaultStreamOffsetConfigured(systemStream)));
    doTestSamzaPropertyDoesNotExist(StreamConfigJava.CONSUMER_OFFSET_DEFAULT,
        (config, systemStream) -> assertFalse(config.isDefaultStreamOffsetConfigured(systemStream)));
    doTestSamzaPropertyInvalidConfig(StreamConfigJava::isDefaultStreamOffsetConfigured);
  }

  @Test
  public void testGetBootstrapEnabled() {
    doTestSamzaProperty(StreamConfigJava.BOOTSTRAP, "true",
        (config, systemStream) -> assertTrue(config.getBootstrapEnabled(systemStream)));
    doTestSamzaProperty(StreamConfigJava.BOOTSTRAP, "false",
        (config, systemStream) -> assertFalse(config.getBootstrapEnabled(systemStream)));
    // if not true/false, then use false
    doTestSamzaProperty(StreamConfigJava.BOOTSTRAP, "unknown_value",
        (config, systemStream) -> assertFalse(config.getBootstrapEnabled(systemStream)));
    doTestSamzaPropertyDoesNotExist(StreamConfigJava.BOOTSTRAP,
        (config, systemStream) -> assertFalse(config.getBootstrapEnabled(systemStream)));
    doTestSamzaPropertyInvalidConfig(StreamConfigJava::getBootstrapEnabled);
  }

  @Test
  public void testGetBroadcastEnabled() {
    doTestSamzaProperty(StreamConfigJava.BROADCAST, "true",
        (config, systemStream) -> assertTrue(config.getBroadcastEnabled(systemStream)));
    doTestSamzaProperty(StreamConfigJava.BROADCAST, "false",
        (config, systemStream) -> assertFalse(config.getBroadcastEnabled(systemStream)));
    // if not true/false, then use false
    doTestSamzaProperty(StreamConfigJava.BROADCAST, "unknown_value",
        (config, systemStream) -> assertFalse(config.getBroadcastEnabled(systemStream)));
    doTestSamzaPropertyDoesNotExist(StreamConfigJava.BROADCAST,
        (config, systemStream) -> assertFalse(config.getBroadcastEnabled(systemStream)));
    doTestSamzaPropertyInvalidConfig(StreamConfigJava::getBroadcastEnabled);
  }

  @Test
  public void testGetPriority() {
    doTestSamzaProperty(StreamConfigJava.PRIORITY, "0",
        (config, systemStream) -> assertEquals(0, config.getPriority(systemStream)));
    doTestSamzaProperty(StreamConfigJava.PRIORITY, "100",
        (config, systemStream) -> assertEquals(100, config.getPriority(systemStream)));
    doTestSamzaProperty(StreamConfigJava.PRIORITY, "-1",
        (config, systemStream) -> assertEquals(-1, config.getPriority(systemStream)));
    doTestSamzaPropertyDoesNotExist(StreamConfigJava.PRIORITY,
        (config, systemStream) -> assertEquals(-1, config.getPriority(systemStream)));
    doTestSamzaPropertyInvalidConfig(StreamConfigJava::getPriority);
  }

  @Test
  public void testGetSerdeStreams() {
    assertEquals(Collections.emptySet(),
        new StreamConfigJava(new MapConfig()).getSerdeStreams(SYSTEM));

    // not key/msg serde property for "streams."
    StreamConfigJava streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + SAMZA_IGNORED_PROPERTY, UNUSED_VALUE)));
    assertEquals(Collections.emptySet(), streamConfig.getSerdeStreams(SYSTEM));

    // not matching system for "streams."
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + StreamConfigJava.KEY_SERDE, UNUSED_VALUE,
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), "otherSystem")));
    assertEquals(Collections.emptySet(), streamConfig.getSerdeStreams(SYSTEM));

    // not key/msg serde property for "systems.<system>.streams."
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + SAMZA_IGNORED_PROPERTY, UNUSED_VALUE)));
    assertEquals(Collections.emptySet(),streamConfig.getSerdeStreams(SYSTEM));

    // not matching system for "systems.<system>.streams."
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, "otherSystem", STREAM_ID) + StreamConfigJava.KEY_SERDE,
        UNUSED_VALUE)));
    assertEquals(Collections.emptySet(), streamConfig.getSerdeStreams(SYSTEM));

    // not matching system for "streams."
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + StreamConfigJava.KEY_SERDE, UNUSED_VALUE,
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), "otherSystem")));
    assertEquals(Collections.emptySet(),streamConfig.getSerdeStreams(SYSTEM));

    String serdeValue = "my.serde.class";

    // key serde for "streams."
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), SYSTEM,
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + StreamConfigJava.KEY_SERDE, serdeValue)));
    assertEquals(Collections.singleton(new SystemStream(SYSTEM, STREAM_ID)), streamConfig.getSerdeStreams(SYSTEM));

    // msg serde for "streams."
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), SYSTEM,
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + StreamConfigJava.MSG_SERDE, serdeValue)));
    assertEquals(Collections.singleton(new SystemStream(SYSTEM, STREAM_ID)), streamConfig.getSerdeStreams(SYSTEM));

    // serde for "streams." with physical stream name mapping
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), SYSTEM,
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM,
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + StreamConfigJava.KEY_SERDE, serdeValue)));
    assertEquals(Collections.singleton(new SystemStream(SYSTEM, PHYSICAL_STREAM)), streamConfig.getSerdeStreams(SYSTEM));

    // key serde for "systems.<system>.streams."
    streamConfig = new StreamConfigJava(new MapConfig(
        ImmutableMap.of(String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + StreamConfigJava.KEY_SERDE,
            serdeValue)));
    assertEquals(Collections.singleton(new SystemStream(SYSTEM, STREAM_ID)), streamConfig.getSerdeStreams(SYSTEM));

    // msg serde for "systems.<system>.streams."
    streamConfig = new StreamConfigJava(new MapConfig(
        ImmutableMap.of(String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + StreamConfigJava.MSG_SERDE,
            serdeValue)));
    assertEquals(Collections.singleton(new SystemStream(SYSTEM, STREAM_ID)), streamConfig.getSerdeStreams(SYSTEM));

    // merge several different ways of providing serdes
    String streamIdWithPhysicalName = "streamIdWithPhysicalName";
    streamConfig = new StreamConfigJava(new MapConfig(new ImmutableMap.Builder<String, String>()
        // need to map the stream ids to the system
        .put(JobConfig.JOB_DEFAULT_SYSTEM, SYSTEM)
        // key and msg serde for "streams."
        .put(String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + StreamConfigJava.KEY_SERDE, serdeValue)
        .put(String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + StreamConfigJava.MSG_SERDE, serdeValue)
        // key serde for "streams." with physical stream name mapping
        .put(String.format(StreamConfigJava.STREAM_ID_PREFIX, streamIdWithPhysicalName) + StreamConfigJava.KEY_SERDE,
            serdeValue)
        .put(String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, streamIdWithPhysicalName), PHYSICAL_STREAM)
        // key serde for "systems.<system>.streams."
        .put(String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, OTHER_STREAM_ID) + StreamConfigJava.KEY_SERDE,
            serdeValue)
        .build()));
    assertEquals(Sets.newHashSet(new SystemStream(SYSTEM, STREAM_ID), new SystemStream(SYSTEM, PHYSICAL_STREAM),
        new SystemStream(SYSTEM, OTHER_STREAM_ID)), streamConfig.getSerdeStreams(SYSTEM));
  }

  @Test
  public void testGetStreamProperties() {
    assertEquals(new MapConfig(), new StreamConfigJava(new MapConfig()).getStreamProperties(STREAM_ID));

    String propertyName = "stream.property.name";

    // BEGIN: tests in which properties cannot be found in the config

    // not matching stream id for "streams."
    StreamConfigJava streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_ID_PREFIX, OTHER_STREAM_ID) + propertyName, UNUSED_VALUE)));
    assertEquals(new MapConfig(), streamConfig.getStreamProperties(STREAM_ID));

    // not matching stream id for "systems.<system>.streams."
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, OTHER_STREAM_ID) + propertyName, UNUSED_VALUE,
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, OTHER_STREAM_ID), SYSTEM)));
    assertEquals(new MapConfig(), streamConfig.getStreamProperties(STREAM_ID));

    // no system mapping when using "systems.<system>.streams."
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + propertyName, UNUSED_VALUE)));
    assertEquals(new MapConfig(), streamConfig.getStreamProperties(STREAM_ID));

    // ignore property with "samza" prefix for "streams."
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + SAMZA_IGNORED_PROPERTY, UNUSED_VALUE)));
    assertEquals(new MapConfig(), streamConfig.getStreamProperties(STREAM_ID));

    // ignore property with "samza" prefix for "streams."
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + SAMZA_IGNORED_PROPERTY, UNUSED_VALUE,
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), SYSTEM)));
    assertEquals(new MapConfig(), streamConfig.getStreamProperties(STREAM_ID));

    // should not map physical name back to stream id if physical name is passed as stream id
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + propertyName, UNUSED_VALUE,
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), SYSTEM,
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM)));
    assertEquals(new MapConfig(), streamConfig.getStreamProperties(PHYSICAL_STREAM));

    // BEGIN: tests in which properties can be found in the config

    String propertyValue = "value";

    // "streams."
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + propertyName, propertyValue)));
    assertEquals(new MapConfig(ImmutableMap.of(propertyName, propertyValue)),
        streamConfig.getStreamProperties(STREAM_ID));

    // "systems.<system>.streams."
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + propertyName, propertyValue,
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), SYSTEM)));
    assertEquals(new MapConfig(ImmutableMap.of(propertyName, propertyValue)),
        streamConfig.getStreamProperties(STREAM_ID));

    // "systems.<system>.default.stream."
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(SystemConfig.SYSTEM_DEFAULT_STREAMS_PREFIX_FORMAT, SYSTEM) + propertyName, propertyValue,
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), SYSTEM)));
    assertEquals(new MapConfig(ImmutableMap.of(propertyName, propertyValue)),
        streamConfig.getStreamProperties(STREAM_ID));

    // use physical name mapping for "systems.<system>.streams."
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, PHYSICAL_STREAM) + propertyName, propertyValue,
        // should not use stream id since there is physical stream
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + propertyName, UNUSED_VALUE,
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), SYSTEM,
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM)));
    assertEquals(new MapConfig(ImmutableMap.of(propertyName, propertyValue)),
        streamConfig.getStreamProperties(STREAM_ID));

    // "streams." should override "systems.<system>.streams."
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + propertyName, propertyValue,
        // should not use "systems.<system>.streams." since there is a "streams." config
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + propertyName, UNUSED_VALUE,
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), SYSTEM)));
    assertEquals(new MapConfig(ImmutableMap.of(propertyName, propertyValue)),
        streamConfig.getStreamProperties(STREAM_ID));

    // "systems.<system>.streams." should override "systems.<system>.default.stream."
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + propertyName, propertyValue,
        // should not use "systems.<system>.default.stream." since there is a "systems.<system>.streams."
        String.format(SystemConfig.SYSTEM_DEFAULT_STREAMS_PREFIX_FORMAT, SYSTEM) + propertyName, UNUSED_VALUE,
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), SYSTEM)));
    assertEquals(new MapConfig(ImmutableMap.of(propertyName, propertyValue)),
        streamConfig.getStreamProperties(STREAM_ID));

    // merge multiple ways of specifying configs
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        // "streams."
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + "from.stream.id.property", "fromStreamIdValue",
        // second "streams." property
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + "from.stream.id.other.property",
        "fromStreamIdOtherValue",
        // "systems.<system>.streams."
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + "from.system.stream.property",
        "fromSystemStreamValue",
        // need to map the stream id to a system
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), SYSTEM)));
    assertEquals(new MapConfig(ImmutableMap.of(
        "from.stream.id.property", "fromStreamIdValue",
        "from.stream.id.other.property", "fromStreamIdOtherValue",
        "from.system.stream.property", "fromSystemStreamValue")),
        streamConfig.getStreamProperties(STREAM_ID));
  }

  @Test
  public void testGetSystem() {
    assertNull(new StreamConfigJava(new MapConfig()).getSystem(STREAM_ID));

    // system is specified directly
    StreamConfigJava streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), SYSTEM,
        JobConfig.JOB_DEFAULT_SYSTEM, "otherSystem",
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, OTHER_STREAM_ID), "otherSystem")));
    assertEquals(SYSTEM, streamConfig.getSystem(STREAM_ID));

    // fall back to job default system
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        JobConfig.JOB_DEFAULT_SYSTEM, SYSTEM,
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, OTHER_STREAM_ID), "otherSystem")));
    assertEquals(SYSTEM, streamConfig.getSystem(STREAM_ID));
  }

  @Test
  public void testGetPhysicalName() {
    assertEquals(STREAM_ID, new StreamConfigJava(new MapConfig()).getPhysicalName(STREAM_ID));

    // ignore mapping for other stream ids
    StreamConfigJava streamConfig = new StreamConfigJava(new MapConfig(
        ImmutableMap.of(String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, OTHER_STREAM_ID), PHYSICAL_STREAM)));
    assertEquals(STREAM_ID, streamConfig.getPhysicalName(STREAM_ID));

    streamConfig = new StreamConfigJava(new MapConfig(
        ImmutableMap.of(String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM)));
    assertEquals(PHYSICAL_STREAM, streamConfig.getPhysicalName(STREAM_ID));
  }

  @Test
  public void testGetIsIntermediateStream() {
    assertFalse(new StreamConfigJava(new MapConfig()).getIsIntermediateStream(STREAM_ID));

    // ignore mapping for other stream ids
    StreamConfigJava streamConfig = new StreamConfigJava(new MapConfig(
        ImmutableMap.of(String.format(StreamConfigJava.IS_INTERMEDIATE_FOR_STREAM_ID, OTHER_STREAM_ID), "true")));
    assertFalse(streamConfig.getIsIntermediateStream(STREAM_ID));

    // do not use stream id property if physical name is passed as input
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.IS_INTERMEDIATE_FOR_STREAM_ID, STREAM_ID), "true",
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM)));
    assertFalse(streamConfig.getIsIntermediateStream(PHYSICAL_STREAM));

    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.IS_INTERMEDIATE_FOR_STREAM_ID, STREAM_ID), "true")));
    assertTrue(streamConfig.getIsIntermediateStream(STREAM_ID));

    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.IS_INTERMEDIATE_FOR_STREAM_ID, STREAM_ID), "false")));
    assertFalse(streamConfig.getIsIntermediateStream(STREAM_ID));
  }

  @Test
  public void testGetDeleteCommittedMessages() {
    assertFalse(new StreamConfigJava(new MapConfig()).getDeleteCommittedMessages(STREAM_ID));

    // ignore mapping for other stream ids
    StreamConfigJava streamConfig = new StreamConfigJava(new MapConfig(
        ImmutableMap.of(String.format(StreamConfigJava.DELETE_COMMITTED_MESSAGES_FOR_STREAM_ID, OTHER_STREAM_ID),
            "true")));
    assertFalse(streamConfig.getDeleteCommittedMessages(STREAM_ID));

    // do not use stream id property if physical name is passed as input
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.DELETE_COMMITTED_MESSAGES_FOR_STREAM_ID, STREAM_ID), "true",
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM)));
    assertFalse(streamConfig.getDeleteCommittedMessages(PHYSICAL_STREAM));

    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.DELETE_COMMITTED_MESSAGES_FOR_STREAM_ID, STREAM_ID), "true")));
    assertTrue(streamConfig.getDeleteCommittedMessages(STREAM_ID));

    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.DELETE_COMMITTED_MESSAGES_FOR_STREAM_ID, STREAM_ID), "false")));
    assertFalse(streamConfig.getDeleteCommittedMessages(STREAM_ID));
  }

  @Test
  public void testGetIsBounded() {
    assertFalse(new StreamConfigJava(new MapConfig()).getIsBounded(STREAM_ID));

    // ignore mapping for other stream ids
    StreamConfigJava streamConfig = new StreamConfigJava(new MapConfig(
        ImmutableMap.of(String.format(StreamConfigJava.IS_BOUNDED_FOR_STREAM_ID, OTHER_STREAM_ID),
            "true")));
    assertFalse(streamConfig.getIsBounded(STREAM_ID));

    // do not use stream id property if physical name is passed as input
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.IS_BOUNDED_FOR_STREAM_ID, STREAM_ID), "true",
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM)));
    assertFalse(streamConfig.getIsBounded(PHYSICAL_STREAM));

    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.IS_BOUNDED_FOR_STREAM_ID, STREAM_ID), "true")));
    assertTrue(streamConfig.getIsBounded(STREAM_ID));

    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.IS_BOUNDED_FOR_STREAM_ID, STREAM_ID), "false")));
    assertFalse(streamConfig.getIsBounded(STREAM_ID));
  }

  @Test
  public void testGetStreamIds() {
    assertEquals(ImmutableList.of(), ImmutableList.copyOf(
       new StreamConfigJava(new MapConfig()).getStreamIds()));

    StreamConfigJava streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + ".property", "value")));
    assertEquals(ImmutableList.of(STREAM_ID),
        ImmutableList.copyOf(streamConfig.getStreamIds()));

    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + ".property.subProperty", "value")));
    assertEquals(ImmutableList.of(STREAM_ID),
        ImmutableList.copyOf(streamConfig.getStreamIds()));

    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + ".property0", "value",
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + ".property1", "value",
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + ".property.subProperty0", "value",
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + ".property.subProperty1", "value",
        String.format(StreamConfigJava.STREAM_ID_PREFIX, OTHER_STREAM_ID) + ".property", "value")));
    assertEquals(ImmutableList.of(STREAM_ID, OTHER_STREAM_ID),
        ImmutableList.copyOf(streamConfig.getStreamIds()));
  }

  private static void doTestSamzaProperty(String propertyName, String propertyValue, SamzaPropertyAssertion assertion) {
    doTestSamzaPropertyAccess(propertyName, propertyValue, assertion);
    doTestSamzaPropertyAccessWithPhysicalStream(propertyName, propertyValue, assertion);
    doTestSamzaPropertyPriority(propertyName, propertyValue, assertion);
    doTestSamzaPropertyMultipleStreams(propertyName, propertyValue, assertion);
  }

  /**
   * Tests for Samza property access using different lookup methods, when using stream id in system stream.
   * This includes tests in which there is no specified physical stream, so the stream id is used as the physical
   * stream.
   */
  private static void doTestSamzaPropertyAccess(String propertyName, String value, SamzaPropertyAssertion assertion) {
    // streams.<streamId>.<property>
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + propertyName, value,
        // all streams need to have a system
        JobConfig.JOB_DEFAULT_SYSTEM, SYSTEM))),
        SYSTEM_STREAM);

    // systems.<system>.streams.<stream>.<property> where stream id has no specified physical stream
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + propertyName, value,
        // specify the system for the stream id
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), SYSTEM))),
        SYSTEM_STREAM);

    // systems.<system>.streams.<stream>.<property> where stream is the streamId, system is from job default system
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + propertyName, value,
        // use job default system to get the system
        JobConfig.JOB_DEFAULT_SYSTEM, SYSTEM))),
        SYSTEM_STREAM);

    // systems.<system>.default.stream.<property> where stream id has no specified physical stream
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(SystemConfig.SYSTEM_DEFAULT_STREAMS_PREFIX_FORMAT, SYSTEM) + propertyName, value,
        // specify the system for the stream id
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), SYSTEM))),
        SYSTEM_STREAM);

    // systems.<system>.streams.<stream>.<property> where no system mapping (fall back to SystemStream)
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + propertyName, value,
        // map the stream id to the physical stream
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM))),
        SYSTEM_STREAM);

    // systems.<system>.default.stream.<property> where no system mapping (fall back to SystemStream)
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(SystemConfig.SYSTEM_DEFAULT_STREAMS_PREFIX_FORMAT, SYSTEM) + propertyName, value,
        // map the stream id to the physical stream
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM))),
        SYSTEM_STREAM);

    // systems.<system>.streams.<stream>.<property> where stream id has a physical name but property is from stream id
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + propertyName, value,
        // use job default system to get the system
        JobConfig.JOB_DEFAULT_SYSTEM, SYSTEM,
        // map the stream id to the physical stream
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM))),
        SYSTEM_STREAM);

    // systems.<system>.default.stream.<property> where stream id has a physical name but property is from stream id
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(SystemConfig.SYSTEM_DEFAULT_STREAMS_PREFIX_FORMAT, SYSTEM) + propertyName, value,
        // use job default system to get the system
        JobConfig.JOB_DEFAULT_SYSTEM, SYSTEM,
        // map the stream id to the physical stream
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM))),
        SYSTEM_STREAM);
  }

  /**
   * Tests for Samza property access using different lookup methods, when using physical stream in system stream.
   */
  private static void doTestSamzaPropertyAccessWithPhysicalStream(String propertyName, String value,
      SamzaPropertyAssertion assertion) {
    // streams.<streamId>.<property>
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + propertyName, value,
        // all streams need to have a system
        JobConfig.JOB_DEFAULT_SYSTEM, SYSTEM,
        // map the stream id to the physical stream
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM))),
        SYSTEM_STREAM_PHYSICAL);

    // systems.<system>.streams.<stream>.<property> with a specific system for the stream id
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, PHYSICAL_STREAM) + propertyName, value,
        // specify the system for the stream id
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), SYSTEM,
        // map the stream id to the physical stream
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM))),
        SYSTEM_STREAM_PHYSICAL);

    // systems.<system>.streams.<stream>.<property> with system coming from job default system
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, PHYSICAL_STREAM) + propertyName, value,
        // use job default system to get the system
        JobConfig.JOB_DEFAULT_SYSTEM, SYSTEM,
        // map the stream id to the physical stream
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM))),
        SYSTEM_STREAM_PHYSICAL);

    // systems.<system>.default.stream.<property>
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(SystemConfig.SYSTEM_DEFAULT_STREAMS_PREFIX_FORMAT, SYSTEM) + propertyName, value,
        // specify the system for the stream id
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), SYSTEM,
        // map the stream id to the physical stream
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM))),
        SYSTEM_STREAM_PHYSICAL);
  }

  /**
   * Tests that certain properties have priority over others. For conciseness, this will not explicitly test priorities
   * for all different ways of specifying a property. This will compare two options at a time, and then we can infer
   * priorities transitively.
   */
  private static void doTestSamzaPropertyPriority(String propertyName, String value, SamzaPropertyAssertion assertion) {
    // streams.<streamId>.<property> vs. systems.<system>.streams.<stream>.<property>
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + propertyName, value,
        // all streams need to have a system
        JobConfig.JOB_DEFAULT_SYSTEM, SYSTEM,
        // this config should not be used
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + propertyName, UNUSED_VALUE))),
        SYSTEM_STREAM);

    // systems.<system>.streams.<stream>.<property> vs. systems.<system>.default.stream.<property>
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + propertyName, value,
        // specify the system for the stream id
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), SYSTEM,
        // this config should not be used
        String.format(SystemConfig.SYSTEM_DEFAULT_STREAMS_PREFIX_FORMAT, SYSTEM) + propertyName, UNUSED_VALUE))),
        SYSTEM_STREAM);

    /*
     * The next logical case to check would be systems.<system>.default.stream.<property> with a system mapping in the
     * config vs. systems.<system>.streams.<stream>.<property> with a system mapping, but that is not possible, so move
     * on to the next case.
     */

    /*
     * systems.<system>.streams.<stream>.<property> without a system mapping in the config vs.
     * systems.<system>.default.stream.<property> without a system mapping in the config
     */
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + propertyName, value,
        // this config should not be used
        String.format(SystemConfig.SYSTEM_DEFAULT_STREAMS_PREFIX_FORMAT, SYSTEM) + propertyName, UNUSED_VALUE))),
        SYSTEM_STREAM);
  }

  /**
   * Tests for Samza property access in which multiple streams are configured.
   * Only testing cases in which streams may share some properties.
   */
  private static void doTestSamzaPropertyMultipleStreams(String propertyName, String value, SamzaPropertyAssertion assertion) {
    // systems.<system>.default.stream.<property> where stream id has no specified physical stream
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(SystemConfig.SYSTEM_DEFAULT_STREAMS_PREFIX_FORMAT, SYSTEM) + propertyName, value,
        // specify the systems for the streams
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, STREAM_ID), SYSTEM,
        String.format(StreamConfigJava.SYSTEM_FOR_STREAM_ID, OTHER_STREAM_ID), SYSTEM))),
        SYSTEM_STREAM);
  }

  private static void doTestSamzaPropertyInvalidConfig(SamzaPropertyLookup lookup) {
    // configure physical stream and have mapping from stream id to physical stream
    StreamConfigJava streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM,
        String.format(StreamConfigJava.STREAM_ID_PREFIX, PHYSICAL_STREAM) + ".property", "value",
        JobConfig.JOB_DEFAULT_SYSTEM, SYSTEM)));
    try {
      lookup.doLookup(streamConfig, SYSTEM_STREAM_PHYSICAL);
      fail("Expected an exception due to having too many mappings to a physical stream");
    } catch (IllegalStateException e) {
      // expected to reach here
    }

    // two separate stream ids map to same physical stream
    streamConfig = new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM,
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, OTHER_STREAM_ID), PHYSICAL_STREAM,
        JobConfig.JOB_DEFAULT_SYSTEM, SYSTEM)));
    try {
      lookup.doLookup(streamConfig, SYSTEM_STREAM_PHYSICAL);
      fail("Expected an exception due to having too many mappings to a physical stream");
    } catch (IllegalStateException e) {
      // expected to reach here
    }
  }

  private static void doTestSamzaPropertyDoesNotExist(String propertyName, SamzaPropertyAssertion assertion) {
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        // just put in some value which will be ignored
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + SAMZA_IGNORED_PROPERTY, UNUSED_VALUE))),
        SYSTEM_STREAM);

    /*
     * Won't use streams.<streamId>.<property> if streamId is mapped to a physical stream
     */
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_ID_PREFIX, STREAM_ID) + propertyName, UNUSED_VALUE,
        // all streams need to have a system
        JobConfig.JOB_DEFAULT_SYSTEM, SYSTEM,
        // map the stream id to the physical stream
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM))),
        SYSTEM_STREAM);

    /*
     * Won't use systems.<system>.streams.<stream>.<property> if stream is mapped to a physical stream and the property
     * is only specified using the physical stream
     */
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, PHYSICAL_STREAM) + propertyName, UNUSED_VALUE,
        // all streams need to have a system
        JobConfig.JOB_DEFAULT_SYSTEM, SYSTEM,
        // map the stream id to the physical stream
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM))),
        SYSTEM_STREAM);

    /*
     * Won't use systems.<system>.streams.<stream>.<property> if stream is mapped to a physical stream and there is no
     * system mapping for the stream id
     */
    assertion.doAssertion(new StreamConfigJava(new MapConfig(ImmutableMap.of(
        String.format(StreamConfigJava.STREAM_PREFIX, SYSTEM, STREAM_ID) + propertyName, UNUSED_VALUE,
        // map the stream id to the physical stream
        String.format(StreamConfigJava.PHYSICAL_NAME_FOR_STREAM_ID, STREAM_ID), PHYSICAL_STREAM))),
        SYSTEM_STREAM_PHYSICAL);
  }

  /**
   * Used to execute assertions for tests which look up specific Samza properties.
   */
  @FunctionalInterface
  private interface SamzaPropertyAssertion {
    void doAssertion(StreamConfigJava streamConfig, SystemStream systemStream);
  }

  /**
   * Used to specify how to look up specific Samza properties.
   */
  @FunctionalInterface
  private interface SamzaPropertyLookup {
    void doLookup(StreamConfigJava streamConfig, SystemStream systemStream);
  }
}