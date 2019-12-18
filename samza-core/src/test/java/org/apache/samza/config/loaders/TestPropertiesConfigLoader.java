package org.apache.samza.config.loaders;

import java.util.Collections;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigLoader;
import org.apache.samza.config.MapConfig;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestPropertiesConfigLoader {
  @Test
  public void testCanReadPropertiesConfigFiles() {
    ConfigLoader loader = new PropertiesConfigLoaderFactory().getLoader(
        new MapConfig(Collections.singletonMap("path", getClass().getResource("/test.properties").getPath())));

    Config config = loader.getConfig();
    assertEquals("bar", config.get("foo"));
  }

  @Test
  public void testCanNotReadWithoutPath() {
    try {
      ConfigLoader loader = new PropertiesConfigLoaderFactory().getLoader(new MapConfig());
      loader.getConfig();
      fail("should have gotten a samza exception");
    } catch (SamzaException e) {
      // Do nothing
    }
  }
}