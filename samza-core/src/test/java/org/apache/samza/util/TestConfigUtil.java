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

package org.apache.samza.util;

import java.util.HashMap;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigRewriter;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.loaders.PropertiesConfigLoaderFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestConfigUtil {
  private static final String CONFIG_KEY = "config.key";
  private static final String CONFIG_VALUE = "value";
  private static final String NEW_CONFIG_KEY = "new.rewritten.config.key";
  private static final String REWRITER_NAME = "propertyRewriter";
  private static final String OTHER_REWRITER_NAME = "otherPropertyRewriter";

  @Test
  public void testRewriteConfig() {
    Map<String, String> baseConfigMap = ImmutableMap.of(CONFIG_KEY, CONFIG_VALUE);

    // no rewriters
    Map<String, String> fullConfig = new HashMap<>(baseConfigMap);
    assertEquals(fullConfig, ConfigUtil.rewriteConfig(new MapConfig(fullConfig)));

    // rewriter that adds property
    fullConfig = new HashMap<>(baseConfigMap);
    fullConfig.put(JobConfig.CONFIG_REWRITERS, REWRITER_NAME);
    fullConfig.put(String.format(JobConfig.CONFIG_REWRITER_CLASS, REWRITER_NAME), NewPropertyRewriter.class.getName());
    Map<String, String> expectedConfigMap = new HashMap<>(fullConfig);
    expectedConfigMap.put(NEW_CONFIG_KEY, CONFIG_VALUE);
    assertEquals(new MapConfig(expectedConfigMap), ConfigUtil.rewriteConfig(new MapConfig(fullConfig)));

    // rewriter that updates property
    fullConfig = new HashMap<>(baseConfigMap);
    fullConfig.put(JobConfig.CONFIG_REWRITERS, REWRITER_NAME);
    fullConfig.put(String.format(JobConfig.CONFIG_REWRITER_CLASS, REWRITER_NAME),
        UpdatePropertyRewriter.class.getName());
    expectedConfigMap = new HashMap<>(fullConfig);
    expectedConfigMap.put(CONFIG_KEY, CONFIG_VALUE + CONFIG_VALUE);
    assertEquals(new MapConfig(expectedConfigMap), ConfigUtil.rewriteConfig(new MapConfig(fullConfig)));

    // rewriter that removes property
    fullConfig = new HashMap<>(baseConfigMap);
    fullConfig.put(JobConfig.CONFIG_REWRITERS, REWRITER_NAME);
    fullConfig.put(String.format(JobConfig.CONFIG_REWRITER_CLASS, REWRITER_NAME),
        DeletePropertyRewriter.class.getName());
    expectedConfigMap = new HashMap<>(fullConfig);
    expectedConfigMap.remove(CONFIG_KEY);
    assertEquals(new MapConfig(expectedConfigMap), ConfigUtil.rewriteConfig(new MapConfig(fullConfig)));

    // only apply rewriters from rewriters list
    fullConfig = new HashMap<>(baseConfigMap);
    fullConfig.put(JobConfig.CONFIG_REWRITERS, OTHER_REWRITER_NAME);
    fullConfig.put(String.format(JobConfig.CONFIG_REWRITER_CLASS, REWRITER_NAME), NewPropertyRewriter.class.getName());
    fullConfig.put(String.format(JobConfig.CONFIG_REWRITER_CLASS, OTHER_REWRITER_NAME),
        UpdatePropertyRewriter.class.getName());
    expectedConfigMap = new HashMap<>(fullConfig);
    expectedConfigMap.put(CONFIG_KEY, CONFIG_VALUE + CONFIG_VALUE);
    assertEquals(new MapConfig(expectedConfigMap), ConfigUtil.rewriteConfig(new MapConfig(fullConfig)));

    // two rewriters; second rewriter overwrites configs from first
    fullConfig = new HashMap<>(baseConfigMap);
    fullConfig.put(JobConfig.CONFIG_REWRITERS, REWRITER_NAME + "," + OTHER_REWRITER_NAME);
    fullConfig.put(String.format(JobConfig.CONFIG_REWRITER_CLASS, REWRITER_NAME), NewPropertyRewriter.class.getName());
    fullConfig.put(String.format(JobConfig.CONFIG_REWRITER_CLASS, OTHER_REWRITER_NAME),
        UpdatePropertyRewriter.class.getName());
    expectedConfigMap = new HashMap<>(fullConfig);
    expectedConfigMap.put(NEW_CONFIG_KEY, CONFIG_VALUE + CONFIG_VALUE);
    assertEquals(new MapConfig(expectedConfigMap), ConfigUtil.rewriteConfig(new MapConfig(fullConfig)));
  }

  /**
   * This fails because Util will interpret the empty string value as a single rewriter which has the empty string as a
   * name, and there is no rewriter class config for a rewriter name which is the empty string.
   * TODO: should this be fixed to interpret the empty string as an empty list?
   */
  @Test(expected = SamzaException.class)
  public void testRewriteConfigConfigRewritersEmptyString() {
    Config config = new MapConfig(ImmutableMap.of(JobConfig.CONFIG_REWRITERS, ""));
    ConfigUtil.rewriteConfig(config);
  }

  @Test(expected = SamzaException.class)
  public void testRewriteConfigNoClassForConfigRewriterName() {
    Config config =
        new MapConfig(ImmutableMap.of(CONFIG_KEY, CONFIG_VALUE, JobConfig.CONFIG_REWRITERS, "unknownRewriter"));
    ConfigUtil.rewriteConfig(config);
  }

  @Test(expected = SamzaException.class)
  public void testRewriteConfigRewriterClassDoesNotExist() {
    Config config = new MapConfig(ImmutableMap.of(CONFIG_KEY, CONFIG_VALUE, JobConfig.CONFIG_REWRITERS, REWRITER_NAME,
        String.format(JobConfig.CONFIG_REWRITER_CLASS, REWRITER_NAME), "not_a_class"));
    ConfigUtil.rewriteConfig(config);
  }

  @Test
  public void testApplyRewriter() {
    // new property
    Map<String, String> fullConfig =
        ImmutableMap.of(CONFIG_KEY, CONFIG_VALUE, String.format(JobConfig.CONFIG_REWRITER_CLASS, REWRITER_NAME),
            NewPropertyRewriter.class.getName());
    Map<String, String> expectedConfigMap = new HashMap<>(fullConfig);
    expectedConfigMap.put(NEW_CONFIG_KEY, CONFIG_VALUE);
    assertEquals(new MapConfig(expectedConfigMap), ConfigUtil.applyRewriter(new MapConfig(fullConfig), REWRITER_NAME));

    // update property
    fullConfig =
        ImmutableMap.of(CONFIG_KEY, CONFIG_VALUE, String.format(JobConfig.CONFIG_REWRITER_CLASS, REWRITER_NAME),
            UpdatePropertyRewriter.class.getName());
    expectedConfigMap = new HashMap<>(fullConfig);
    expectedConfigMap.put(CONFIG_KEY, CONFIG_VALUE + CONFIG_VALUE);
    assertEquals(new MapConfig(expectedConfigMap), ConfigUtil.applyRewriter(new MapConfig(fullConfig), REWRITER_NAME));

    // remove property
    fullConfig =
        ImmutableMap.of(CONFIG_KEY, CONFIG_VALUE, String.format(JobConfig.CONFIG_REWRITER_CLASS, REWRITER_NAME),
            DeletePropertyRewriter.class.getName());
    expectedConfigMap = new HashMap<>(fullConfig);
    expectedConfigMap.remove(CONFIG_KEY);
    assertEquals(new MapConfig(expectedConfigMap), ConfigUtil.applyRewriter(new MapConfig(fullConfig), REWRITER_NAME));
  }

  @Test(expected = SamzaException.class)
  public void testApplyRewriterNoClassForConfigRewriterName() {
    Map<String, String> fullConfig = ImmutableMap.of(CONFIG_KEY, CONFIG_VALUE);
    ConfigUtil.applyRewriter(new MapConfig(fullConfig), REWRITER_NAME);
  }

  @Test(expected = SamzaException.class)
  public void testApplyRewriterClassDoesNotExist() {
    Map<String, String> fullConfig =
        ImmutableMap.of(CONFIG_KEY, CONFIG_VALUE, String.format(JobConfig.CONFIG_REWRITER_CLASS, REWRITER_NAME),
            "not_a_class");
    Config expectedConfig = new MapConfig(ImmutableMap.of(CONFIG_KEY, CONFIG_VALUE, NEW_CONFIG_KEY, CONFIG_VALUE));
    assertEquals(expectedConfig, ConfigUtil.applyRewriter(new MapConfig(fullConfig), REWRITER_NAME));
  }

  @Test
  public void testLoadConfigWithoutLoader() {
    Map<String, String> config = new HashMap<>();
    config.put(JobConfig.JOB_NAME, "new-test-job");

    Config actual = ConfigUtil.loadConfig(new MapConfig(config));

    assertEquals(config.size(), actual.size());
    assertEquals("new-test-job", actual.get(JobConfig.JOB_NAME));
  }

  @Test
  public void testLoadConfigWithLoader() {
    Map<String, String> config = new HashMap<>();
    config.put(JobConfig.CONFIG_LOADER_FACTORY, PropertiesConfigLoaderFactory.class.getCanonicalName());
    config.put(JobConfig.JOB_NAME, "new-test-job");
    config.put(PropertiesConfigLoaderFactory.CONFIG_LOADER_PROPERTIES_PREFIX + "path", getClass().getResource("/test.properties").getPath());

    Config actual = ConfigUtil.loadConfig(new MapConfig(config));

    assertEquals("org.apache.samza.job.MockJobFactory", actual.get("job.factory.class"));
    assertEquals("new-test-job", actual.get("job.name"));
    assertEquals("bar", actual.get("foo"));
  }

  /**
   * Adds a new config entry for the key {@link #NEW_CONFIG_KEY} which has the same value as {@link #CONFIG_KEY}.
   */
  public static class NewPropertyRewriter implements ConfigRewriter {
    @Override
    public Config rewrite(String name, Config config) {
      ImmutableMap.Builder<String, String> newConfigMapBuilder = new ImmutableMap.Builder<>();
      newConfigMapBuilder.putAll(config);
      newConfigMapBuilder.put(NEW_CONFIG_KEY, config.get(CONFIG_KEY));
      return new MapConfig(newConfigMapBuilder.build());
    }
  }

  /**
   * If an entry at {@link #NEW_CONFIG_KEY} exists, overwrites it to be the value concatenated with itself. Otherwise,
   * updates the entry at {@link #CONFIG_KEY} to be the value concatenated to itself.
   */
  public static class UpdatePropertyRewriter implements ConfigRewriter {
    @Override
    public Config rewrite(String name, Config config) {
      Map<String, String> newConfigMap = new HashMap<>(config);
      if (config.containsKey(NEW_CONFIG_KEY)) {
        // for testing overwriting of new configs
        newConfigMap.put(NEW_CONFIG_KEY, config.get(NEW_CONFIG_KEY) + config.get(NEW_CONFIG_KEY));
      } else {
        newConfigMap.put(CONFIG_KEY, config.get(CONFIG_KEY) + config.get(CONFIG_KEY));
      }
      return new MapConfig(newConfigMap);
    }
  }

  /**
   * Removes config entry for the key {@link #CONFIG_KEY} and {@link #NEW_CONFIG_KEY}.
   */
  public static class DeletePropertyRewriter implements ConfigRewriter {
    @Override
    public Config rewrite(String name, Config config) {
      Map<String, String> newConfigMap = new HashMap<>(config);
      newConfigMap.remove(CONFIG_KEY);
      return new MapConfig(newConfigMap);
    }
  }
}
