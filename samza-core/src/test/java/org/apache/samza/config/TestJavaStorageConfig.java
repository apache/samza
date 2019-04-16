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
import java.util.List;
import java.util.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.SamzaException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestJavaStorageConfig {
  private static final String STORE_NAME0 = "store0";
  private static final String STORE_NAME1 = "store1";

  @Test
  public void testGetStoreNames() {
    // empty config, so no stores
    assertEquals(Collections.emptyList(), new JavaStorageConfig(new MapConfig()).getStoreNames());

    // has stores
    JavaStorageConfig storageConfig = new JavaStorageConfig(new MapConfig(
        ImmutableMap.of(String.format(JavaStorageConfig.FACTORY, STORE_NAME0), "store0.factory.class",
            String.format(JavaStorageConfig.FACTORY, STORE_NAME1), "store1.factory.class")));
    List<String> actual = storageConfig.getStoreNames();
    // ordering shouldn't matter
    assertEquals(2, actual.size());
    assertEquals(ImmutableSet.of(STORE_NAME0, STORE_NAME1), ImmutableSet.copyOf(actual));
  }

  @Test
  public void testGetChangelogStream() {
    // empty config, so no changelog stream
    assertEquals(Optional.empty(), new JavaStorageConfig(new MapConfig()).getChangelogStream(STORE_NAME0));

    // store has empty string for changelog stream
    JavaStorageConfig storageConfig = new JavaStorageConfig(
        new MapConfig(ImmutableMap.of(String.format(JavaStorageConfig.CHANGELOG_STREAM, STORE_NAME0), "")));
    assertEquals(Optional.empty(), storageConfig.getChangelogStream(STORE_NAME0));

    // store has full changelog system-stream defined
    storageConfig = new JavaStorageConfig(new MapConfig(
        ImmutableMap.of(String.format(JavaStorageConfig.CHANGELOG_STREAM, STORE_NAME0),
            "changelog-system.changelog-stream0")));
    assertEquals(Optional.of("changelog-system.changelog-stream0"), storageConfig.getChangelogStream(STORE_NAME0));

    // store has changelog stream defined, but system comes from job.changelog.system
    storageConfig = new JavaStorageConfig(new MapConfig(
        ImmutableMap.of(String.format(JavaStorageConfig.CHANGELOG_STREAM, STORE_NAME0), "changelog-stream0",
            JavaStorageConfig.CHANGELOG_SYSTEM, "changelog-system")));
    assertEquals(Optional.of("changelog-system.changelog-stream0"), storageConfig.getChangelogStream(STORE_NAME0));

    // batch mode: create unique stream name
    storageConfig = new JavaStorageConfig(new MapConfig(
        ImmutableMap.of(String.format(JavaStorageConfig.CHANGELOG_STREAM, STORE_NAME0),
            "changelog-system.changelog-stream0", ApplicationConfig.APP_MODE,
            ApplicationConfig.ApplicationMode.BATCH.name().toLowerCase(), ApplicationConfig.APP_RUN_ID, "run-id")));
    assertEquals(Optional.of("changelog-system.changelog-stream0-run-id"),
        storageConfig.getChangelogStream(STORE_NAME0));
  }

  @Test(expected = SamzaException.class)
  public void testGetChangelogStreamMissingSystem() {
    JavaStorageConfig storageConfig = new JavaStorageConfig(new MapConfig(
        ImmutableMap.of(String.format(JavaStorageConfig.CHANGELOG_STREAM, STORE_NAME0), "changelog-stream0")));
    storageConfig.getChangelogStream(STORE_NAME0);
  }

  @Test
  public void testGetAccessLogEnabled() {
    // empty config, access log disabled
    assertFalse(new JavaStorageConfig(new MapConfig()).getAccessLogEnabled(STORE_NAME0));

    assertFalse(new JavaStorageConfig(new MapConfig(
        ImmutableMap.of(String.format(JavaStorageConfig.ACCESSLOG_ENABLED, STORE_NAME0), "false"))).getAccessLogEnabled(
        STORE_NAME0));

    assertTrue(new JavaStorageConfig(new MapConfig(
        ImmutableMap.of(String.format(JavaStorageConfig.ACCESSLOG_ENABLED, STORE_NAME0), "true"))).getAccessLogEnabled(
        STORE_NAME0));
  }

  @Test
  public void testGetAccessLogStream() {
    String changelogStream = "changelog-stream";
    assertEquals(changelogStream + "-" + JavaStorageConfig.ACCESSLOG_STREAM_SUFFIX,
        new JavaStorageConfig(new MapConfig()).getAccessLogStream(changelogStream));
  }

  @Test
  public void testGetAccessLogSamplingRatio() {
    // empty config, return default sampling ratio
    assertEquals(JavaStorageConfig.DEFAULT_ACCESSLOG_SAMPLING_RATIO,
        new JavaStorageConfig(new MapConfig()).getAccessLogSamplingRatio(STORE_NAME0));

    assertEquals(40, new JavaStorageConfig(new MapConfig(
        ImmutableMap.of(String.format(JavaStorageConfig.ACCESSLOG_SAMPLING_RATIO, STORE_NAME0),
            "40"))).getAccessLogSamplingRatio(STORE_NAME0));
  }

  @Test
  public void testGetStorageFactoryClassName() {
    // empty config, so no factory
    assertEquals(Optional.empty(), new JavaStorageConfig(new MapConfig()).getStorageFactoryClassName(STORE_NAME0));

    JavaStorageConfig storageConfig = new JavaStorageConfig(
        new MapConfig(ImmutableMap.of(String.format(JavaStorageConfig.FACTORY, STORE_NAME0), "my.factory.class")));
    assertEquals(Optional.of("my.factory.class"), storageConfig.getStorageFactoryClassName(STORE_NAME0));
  }

  @Test
  public void testGetStorageKeySerde() {
    // empty config, so no key serde
    assertEquals(Optional.empty(), new JavaStorageConfig(new MapConfig()).getStorageKeySerde(STORE_NAME0));

    JavaStorageConfig storageConfig = new JavaStorageConfig(
        new MapConfig(ImmutableMap.of(String.format(JavaStorageConfig.KEY_SERDE, STORE_NAME0), "my.key.serde.class")));
    assertEquals(Optional.of("my.key.serde.class"), storageConfig.getStorageKeySerde(STORE_NAME0));
  }

  @Test
  public void testGetStorageMsgSerde() {
    // empty config, so no msg serde
    assertEquals(Optional.empty(), new JavaStorageConfig(new MapConfig()).getStorageMsgSerde(STORE_NAME0));

    JavaStorageConfig storageConfig = new JavaStorageConfig(
        new MapConfig(ImmutableMap.of(String.format(JavaStorageConfig.MSG_SERDE, STORE_NAME0), "my.msg.serde.class")));
    assertEquals(Optional.of("my.msg.serde.class"), storageConfig.getStorageMsgSerde(STORE_NAME0));
  }

  @Test
  public void testGetChangelogSystem() {
    // empty config, so no system
    assertEquals(Optional.empty(), new JavaStorageConfig(new MapConfig()).getChangelogSystem());

    // job.changelog.system takes precedence over job.default.system
    JavaStorageConfig storageConfig = new JavaStorageConfig(new MapConfig(
        ImmutableMap.of(JavaStorageConfig.CHANGELOG_SYSTEM, "changelog-system", JobConfig.JOB_DEFAULT_SYSTEM(),
            "should-not-be-used")));
    assertEquals(Optional.of("changelog-system"), storageConfig.getChangelogSystem());

    // fall back to job.default.system if job.changelog.system is not specified
    storageConfig =
        new JavaStorageConfig(new MapConfig(ImmutableMap.of(JobConfig.JOB_DEFAULT_SYSTEM(), "default-system")));
    assertEquals(Optional.of("default-system"), storageConfig.getChangelogSystem());
  }

  @Test
  public void testGetSideInputs() {
    // empty config, so no system
    assertEquals(Collections.emptyList(), new JavaStorageConfig(new MapConfig()).getSideInputs(STORE_NAME0));

    // single side input
    JavaStorageConfig storageConfig = new JavaStorageConfig(
        new MapConfig(ImmutableMap.of(String.format(JavaStorageConfig.SIDE_INPUTS, STORE_NAME0), "side-input")));
    assertEquals(Collections.singletonList("side-input"), storageConfig.getSideInputs(STORE_NAME0));

    // multiple side inputs
    storageConfig = new JavaStorageConfig(new MapConfig(
        ImmutableMap.of(String.format(JavaStorageConfig.SIDE_INPUTS, STORE_NAME0), "side-input0,side-input1")));
    assertEquals(ImmutableList.of("side-input0", "side-input1"), storageConfig.getSideInputs(STORE_NAME0));

    // ignore whitespace
    storageConfig = new JavaStorageConfig(new MapConfig(
        ImmutableMap.of(String.format(JavaStorageConfig.SIDE_INPUTS, STORE_NAME0), ", side-input0 ,,side-input1,")));
    assertEquals(ImmutableList.of("side-input0", "side-input1"), storageConfig.getSideInputs(STORE_NAME0));
  }

  @Test
  public void testGetSideInputsProcessorFactory() {
    // empty config, so no factory
    assertEquals(Optional.empty(), new JavaStorageConfig(new MapConfig()).getSideInputsProcessorFactory(STORE_NAME0));

    JavaStorageConfig storageConfig = new JavaStorageConfig(new MapConfig(
        ImmutableMap.of(String.format(JavaStorageConfig.SIDE_INPUTS_PROCESSOR_FACTORY, STORE_NAME0),
            "my.side.inputs.factory.class")));
    assertEquals(Optional.of("my.side.inputs.factory.class"), storageConfig.getSideInputsProcessorFactory(STORE_NAME0));
  }

  @Test
  public void testGetSideInputsProcessorSerializedInstance() {
    // empty config, so no factory
    assertEquals(Optional.empty(),
        new JavaStorageConfig(new MapConfig()).getSideInputsProcessorSerializedInstance(STORE_NAME0));

    JavaStorageConfig storageConfig = new JavaStorageConfig(new MapConfig(
        ImmutableMap.of(String.format(JavaStorageConfig.SIDE_INPUTS_PROCESSOR_SERIALIZED_INSTANCE, STORE_NAME0),
            "serialized_instance")));
    assertEquals(Optional.of("serialized_instance"),
        storageConfig.getSideInputsProcessorSerializedInstance(STORE_NAME0));
  }

  @Test
  public void testGetChangeLogDeleteRetentionInMs() {
    // empty config, return default sampling ratio
    assertEquals(JavaStorageConfig.DEFAULT_CHANGELOG_DELETE_RETENTION_MS,
        new JavaStorageConfig(new MapConfig()).getChangeLogDeleteRetentionInMs(STORE_NAME0));

    JavaStorageConfig storageConfig = new JavaStorageConfig(new MapConfig(
        ImmutableMap.of(String.format(JavaStorageConfig.CHANGELOG_DELETE_RETENTION_MS, STORE_NAME0),
            Long.toString(JavaStorageConfig.DEFAULT_CHANGELOG_DELETE_RETENTION_MS * 2))));
    assertEquals(JavaStorageConfig.DEFAULT_CHANGELOG_DELETE_RETENTION_MS * 2,
        storageConfig.getChangeLogDeleteRetentionInMs(STORE_NAME0));
  }

  @Test
  public void testIsChangelogSystem() {
    JavaStorageConfig storageConfig = new JavaStorageConfig(new MapConfig(ImmutableMap.of(
        // store0 has a changelog stream
        String.format(JavaStorageConfig.FACTORY, STORE_NAME0), "factory.class",
        String.format(JavaStorageConfig.CHANGELOG_STREAM, STORE_NAME0), "system0.changelog-stream",
        // store1 does not have a changelog stream
        String.format(JavaStorageConfig.FACTORY, STORE_NAME1), "factory.class")));
    assertTrue(storageConfig.isChangelogSystem("system0"));
    assertFalse(storageConfig.isChangelogSystem("other-system"));
  }

  @Test
  public void testHasDurableStores() {
    // no changelog, which means no durable stores
    JavaStorageConfig storageConfig = new JavaStorageConfig(
        new MapConfig(ImmutableMap.of(String.format(JavaStorageConfig.FACTORY, STORE_NAME0), "factory.class")));
    assertFalse(storageConfig.hasDurableStores());

    storageConfig = new JavaStorageConfig(new MapConfig(
        ImmutableMap.of(String.format(JavaStorageConfig.FACTORY, STORE_NAME0), "factory.class",
            String.format(JavaStorageConfig.CHANGELOG_STREAM, STORE_NAME0), "system0.changelog-stream")));
    assertTrue(storageConfig.hasDurableStores());
  }
}
