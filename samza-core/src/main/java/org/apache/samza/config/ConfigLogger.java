package org.apache.samza.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * This logger logs the config line by line, while shortening the lines which are too long (over maxLen)
 * Logging preserves the original logger name and may be redirected to a different file
 * by specifying a logger name that starts with ConfigLogger. in log4j configuration.
 */
public class ConfigLogger {
  private final Logger log;
  private final int maxLen;

  public enum Level { INFO, DEBUG, WARN, ERROR}

  public static ConfigLogger of(String loggerName, int maxLen) { return new ConfigLogger(loggerName, maxLen);}

  private ConfigLogger(String loggerName, int maxLen) {
    this.log = LoggerFactory.getLogger("ConfigLogger." + loggerName);
    this.maxLen = maxLen;
  }

  /**
   * log the config line by line
   * @param header prepend the config with this header
   * @param config config to log
   * @param level at what level {@ConfigLogger.Leevel}
   */
  public void logConfig(String header, Config config, Level level) {
    String msg = buildConfigString(header, maxLen, config);
    log(log, msg, level);
  }


  // static api
  public static void logConfig(String loggerName, String header, Config config, int maxLen, Level level) {
    Logger logger = LoggerFactory.getLogger("ConfigLogger." + loggerName);
    String msg = buildConfigString(header, maxLen, config);
    log(logger, msg, level);
  }

  private static String buildConfigString(String header, int maxLen, Config config) {
    StringBuilder bldr = new StringBuilder(header + "\n");
    for (MapConfig.Entry e: config.entrySet()) {
      String val;
      if (e.getValue().toString().length() > maxLen) {
        val = e.getValue().toString().substring(0, maxLen) + "<...>";
      } else {
        val = e.getValue().toString();
      }
      bldr.append(String.format("\t%s = %s\n", e.getKey(), val));
    }
    return bldr.toString();
  }

  private static void log(Logger logger, String msg, Level level) {
    switch (level) {
      case INFO:
        logger.info(msg); break;
      case DEBUG:
        logger.debug(msg); break;
      case WARN:
        logger.warn(msg); break;
      case ERROR:
        logger.error(msg); break;
    }
  }
}
