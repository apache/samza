package org.apache.samza.config.loaders;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigLoader;
import org.apache.samza.config.ConfigLoaderFactory;


public class PropertiesConfigLoaderFactory implements ConfigLoaderFactory {
  private static final String KEY = "path";

  @Override
  public ConfigLoader getLoader(Config config) {
    String path = config.get(KEY);

    if (path == null) {
      throw new SamzaException("path is required to read config from properties file");
    }

    return new PropertiesConfigLoader(path);
  }
}
