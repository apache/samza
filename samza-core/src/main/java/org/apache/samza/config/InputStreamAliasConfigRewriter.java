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

import org.apache.samza.SamzaException;

import java.text.ParseException;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

/**
 * A InputStreamAliasConfigRewriter looks for task input streams configuration. If a input stream is given with alias name
 * it changes the alias name with original input stream name in every configuration where the stream appears.
 * This is useful when you need to change your job's input streams. Changing input stream name requires additional changes
 * through the configuration (if serialization, bootstrapping, offset etc. is defined for a input stream)
 * <p>
 * Example config :
 * task.inputs=kafka.my-stream-1 as broad-alias
 * task.broadcast.inputs=kafka.broad-alias#0
 * systems.kafka.streams.broad-alias.samza.key.serde=string
 * systems.kafka.streams.broad-alias.samza.msg.serde=json
 * systems.kafka.streams.broad-alias.samza.bootstrap=true
 * systems.kafka.streams.broad-alias.samza.offset.default=oldest
 * <p>
 * Rewriten config :
 * task.inputs=kafka.my-stream-1
 * task.broadcast.inputs=kafka.my-stream-1#0
 * systems.kafka.streams.my-stream-1.samza.key.serde=string
 * systems.kafka.streams.my-stream-1.samza.msg.serde=json
 * systems.kafka.streams.my-stream-1.samza.bootstrap=true
 * systems.kafka.streams.my-stream-1.samza.offset.default=oldest
 * <p>
 * If you later change name for the input topic you just need to change your stream name in "task.inputs" configuration.
 * Important! Alias names should not be some of samza's configuration "key words" like "samza",
 * "streams", "systems", "task", "broadcast", etc...
 */
public class InputStreamAliasConfigRewriter implements ConfigRewriter {

  public Config rewrite(Config config) {
    return this.rewrite(this.getClass().getName(), config);
  }

  @Override
  public Config rewrite(String name, Config config) {
    String inputStreamsConfig = config.get(TaskConfig.INPUT_STREAMS());
    if (inputStreamsConfig == null || !inputStreamsConfig.contains(" as "))
      return config;
    String[] inputStreams = inputStreamsConfig.split(",");
    Map<String, String> aliases = new HashMap<>();

    for (String inputStream : inputStreams) {
      inputStream = inputStream.trim();
      if (inputStream.contains(" as ")) {
        String[] entry = inputStream.split(" as ");
        if (entry.length != 2)
          throw new SamzaException(new ParseException("Could not parse alias for input stream.", inputStreamsConfig.indexOf(entry[0])));
        String[] original = entry[0].split("\\.");
        if (original.length != 2)
          throw new SamzaException(new ParseException("Could not parse alias for input stream. Original stream should be in format 'system-name.stream-name'", inputStreamsConfig.indexOf(entry[0])));
        aliases.put(entry[1], original[1]);
      }
    }
    Map<String, String> overwrittenMappings = new HashMap<>();
    Set<String> keysToRemove = new HashSet<>();

    if (!aliases.isEmpty()) {
      for (Map.Entry<String, String> entry : config.entrySet()) {
        String newKey = entry.getKey();
        String newValue = entry.getValue();

        for (Map.Entry<String, String> aliasEntry : aliases.entrySet()) {
          String alias = aliasEntry.getKey();
          String original = aliasEntry.getValue();
          if (newKey.equals(TaskConfig.INPUT_STREAMS())) {
            newValue = newValue.replace(" as " + alias, "");
            overwrittenMappings.put(newKey, newValue);
            continue;
          }
          if (valueWordCheck(newValue, alias)) {
            newValue = newValue.replaceAll(alias, original);
            overwrittenMappings.put(newKey, newValue);
          }
          if (keyWordCheck(newKey, alias)) {
            keysToRemove.add(newKey);
            newKey = newKey.replaceAll(alias, original);
            overwrittenMappings.put(newKey, newValue);
          }
        }
      }
      Map<String, String> rewrittenConfig = new HashMap<>(config);
      keysToRemove.forEach(key -> {
          overwrittenMappings.remove(key);
          rewrittenConfig.remove(key);
        });
      rewrittenConfig.putAll(overwrittenMappings);
      return new MapConfig(rewrittenConfig);
    } else
      return config;
  }

  /**
   * Checks if an alias is a 'word' in config's key.
   * If key does not contain alias then {@code false} is returned.
   * If key contains alias, the method checks if it's separated with '.' characters or if it's at the end of key string.
   * <p>
   * Example:
   * if alias is {@code road} and key is {@code task.broadcast.inputs} the method would return {@code false}
   * if key is {@code systems.kafka.streams.road.samza.key.serde} the method would return {@code true}
   *
   * @param key   configs key string
   * @param alias alias used for input stream
   * @return true if alias is a 'word', false if not
   */
  private boolean keyWordCheck(String key, String alias) {
    int startIndex = key.indexOf(alias);
    if (startIndex == -1)
      return false;
    int endIndex = startIndex + alias.length() - 1;
    char precedingCharacter = startIndex == 0 ? 0 : key.charAt(startIndex - 1);
    char succeedingCharacter = key.length() == endIndex - 1 ? 0 : key.charAt(endIndex + 1);
    if (startIndex == 0) {
      if (succeedingCharacter == '.')
        return true;
      else {
        String word = key.substring(startIndex, key.indexOf('.', endIndex));
        throw new SamzaException("Alias for input stream ( '" + alias + "') cannot be a part of config word ('" + word + "')");
      }
    } else {
      if (precedingCharacter == '.' && (succeedingCharacter == '.' || succeedingCharacter == 0))
        return true;
      else {
        int endOfWordIndex = key.indexOf('.', endIndex);
        endOfWordIndex = endOfWordIndex == -1 ? key.length() - 1 : endOfWordIndex;
        String word = key.substring(startIndex, endOfWordIndex);
        throw new SamzaException("Alias for input stream ( '" + alias + "') cannot be a part of config key word ('" + word + "')");
      }
    }
  }

  /**
   * Checks if an alias is a 'word' in config's value.
   * If value does not contain alias then {@code false} is returned.
   * If value contains alias, the method checks if it's separated with '.' and '#' or is at the end of string.
   *
   * @param value configs value string
   * @param alias alias used for input stream
   * @return true if alias is a 'word', false if not
   */
  private boolean valueWordCheck(String value, String alias) {
    int startIndex = value.indexOf(alias);
    if (startIndex == -1)
      return false;
    int endIndex = startIndex + alias.length() - 1;
    char precedingCharacter = startIndex == 0 ? 0 : value.charAt(startIndex - 1);
    char succeedingCharacter = value.length() == endIndex - 1 ? 0 : value.charAt(endIndex + 1);
    if (precedingCharacter == '.' && (succeedingCharacter == 0 || succeedingCharacter == '#')) {
      return true;
    } else {
      int hashIndex = value.indexOf('#', endIndex);
      int endOfWordIndex = hashIndex == -1 ? value.length() - 1 : hashIndex;
      String word = value.substring(startIndex, endOfWordIndex);
      throw new SamzaException("Alias for input stream ('" + alias + "') cannot be a part of config value word ('" + word + "')");
    }
  }
}
