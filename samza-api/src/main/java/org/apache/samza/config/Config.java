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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Store and retrieve named, typed values as configuration for classes implementing this interface.
 */
public abstract class Config implements Map<String, String> {
  public static final String SENSITIVE_PREFIX = "sensitive.";
  public static final String SENSITIVE_MASK = "********";

  public Config subset(String prefix) {
    return subset(prefix, true);
  }

  public Config subset(String prefix, boolean stripPrefix) {
    Map<String, String> out = new HashMap<String, String>();

    for (Entry<String, String> entry : entrySet()) {
      String k = entry.getKey();
      if (k != null && k.startsWith(prefix)) {
        if (stripPrefix) {
          k = k.substring(prefix.length());
        }

        out.put(k, entry.getValue());
      }
    }

    return new MapConfig(out);
  }

  public Config regexSubset(String regex) {
    Map<String, String> out = new HashMap<String, String>();
    Pattern pattern = Pattern.compile(regex);

    for (Entry<String, String> entry : entrySet()) {
      String k = entry.getKey();
      Matcher matcher = pattern.matcher(k);
      if (matcher.find()) {
        out.put(k, entry.getValue());
      }
    }

    return new MapConfig(out);
  }

  public String get(String k, String defaultString) {
    if (!containsKey(k)) {
      return defaultString;
    }
    return get(k);
  }

  public boolean getBoolean(String k, boolean defaultValue) {
    if (containsKey(k))
      return "true".equalsIgnoreCase(get(k));
    else
      return defaultValue;
  }

  public boolean getBoolean(String k) {
    if (containsKey(k))
      return "true".equalsIgnoreCase(get(k));
    else
      throw new ConfigException("Missing key " + k + ".");
  }

  public short getShort(String k, short defaultValue) {
    if (containsKey(k))
      return Short.parseShort(get(k));
    else
      return defaultValue;
  }

  public short getShort(String k) {
    if (containsKey(k))
      return Short.parseShort(get(k));
    else
      throw new ConfigException("Missing key " + k + ".");
  }

  public long getLong(String k, long defaultValue) {
    if (containsKey(k))
      return Long.parseLong(get(k));
    else
      return defaultValue;
  }

  public long getLong(String k) {
    if (containsKey(k))
      return Long.parseLong(get(k));
    else
      throw new ConfigException("Missing key " + k + ".");
  }

  public int getInt(String k, int defaultValue) {
    if (containsKey(k))
      return Integer.parseInt(get(k));
    else
      return defaultValue;
  }

  public int getInt(String k) {
    if (containsKey(k))
      return Integer.parseInt(get(k));
    else
      throw new ConfigException("Missing key " + k + ".");
  }

  public double getDouble(String k, double defaultValue) {
    if (containsKey(k))
      return Double.parseDouble(get(k));
    else
      return defaultValue;
  }

  public double getDouble(String k) {
    if (containsKey(k))
      return Double.parseDouble(get(k));
    else
      throw new ConfigException("Missing key " + k + ".");
  }

  public List<String> getList(String k, List<String> defaultValue) {
    if (!containsKey(k))
      return defaultValue;

    String value = get(k);
    String[] pieces = value.split("\\s*,\\s*");
    return Arrays.asList(pieces);
  }

  public List<String> getList(String k) {
    if (!containsKey(k))
      throw new ConfigException("Missing key " + k + ".");
    return getList(k, null);
  }

  @SuppressWarnings("unchecked")
  public <T> Class<T> getClass(String k) {
    if (containsKey(k)) {
      try {
        return (Class<T>) Class.forName(get(k));
      } catch (Exception e) {
        throw new ConfigException("Unable to find class.", e);
      }
    } else {
      throw new ConfigException("Missing key " + k + ".");
    }
  }

  @SuppressWarnings("unchecked")
  public <T> T getNewInstance(String k) {
    try {
      return (T) getClass(k).newInstance();
    } catch (Exception e) {
      throw new ConfigException("Unable to instantiate class.", e);
    }
  }

  public Date getDate(String k) {
    return getDate(k, new SimpleDateFormat());
  }

  public Date getDate(String k, String format) {
    return getDate(k, new SimpleDateFormat(format));
  }

  public Date getDate(String k, SimpleDateFormat format) {
    if (!containsKey(k))
      throw new ConfigException("Missing key " + k + ".");

    try {
      return format.parse(get(k));
    } catch (ParseException e) {
      throw new ConfigException("Date format exception.", e);
    }
  }

  public Date getDate(String k, Date defaultValue) {
    return getDate(k, new SimpleDateFormat(), defaultValue);
  }

  public Date getDate(String k, String format, Date defaultValue) {
    return getDate(k, new SimpleDateFormat(format), defaultValue);
  }

  public Date getDate(String k, SimpleDateFormat format, Date defaultValue) {
    if (!containsKey(k))
      return defaultValue;

    try {
      return format.parse(get(k));
    } catch (ParseException e) {
      throw new ConfigException("Date format exception.", e);
    }
  }

  public abstract Config sanitize();

  public void clear() {
    throw new ConfigException("Config is immutable.");
  }

  public String put(String key, String value) {
    throw new ConfigException("Config is immutable.");
  }

  public void putAll(Map<? extends String, ? extends String> m) {
    throw new ConfigException("Config is immutable.");
  }

  public String remove(Object s) {
    throw new ConfigException("Config is immutable.");
  }
}
