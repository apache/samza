package org.apache.samza.config.loaders;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigLoader;
import org.apache.samza.config.MapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PropertiesConfigLoader implements ConfigLoader {
  private static final Logger LOG = LoggerFactory.getLogger(PropertiesConfigLoader.class);
  private final Config config;

  public PropertiesConfigLoader(Config config) {
    this.config = config;
  }

  @Override
  public Config getConfig() {
    String path = config.get("config.path");
    if (path == null) {
      throw new SamzaException("path is required to read config from properties file");
    }

    try {
      InputStream in = new FileInputStream(path);
      Properties props = new Properties();

      props.load(in);
      in.close();

      LOG.debug("got config {} from config {}", props, path);

      return new MapConfig(props);
    } catch (IOException e) {
      throw new SamzaException("Failed to read from");
    }
  }
}
