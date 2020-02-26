package org.apache.samza.lineage;

import java.util.Optional;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.LineageConfig;
import org.apache.samza.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The LineageEmitter class helps generate and emit job lineage data to specified sink stream.
 */
public final class LineageEmitter {

  public static final Logger LOGGER = LoggerFactory.getLogger(LineageEmitter.class);

  /**
   * Emit the job lineage information to specified sink stream.
   * @param config Samza job config
   */
  public static void emit(Config config) {
    LineageConfig lineageConfig = new LineageConfig(config);
    Optional<String> lineageFactoryClassName = lineageConfig.getLineageFactoryClassName();
    Optional<String> lineageReporterFactoryClassName = lineageConfig.getLineageReporterFactoryClassName();

    if (!lineageFactoryClassName.isPresent() && !lineageReporterFactoryClassName.isPresent()) {
      return;
    }
    if (!lineageFactoryClassName.isPresent()) {
      LOGGER.warn("Missing the config: {}, skip to enable lineage feature", LineageConfig.LINEAGE_FACTORY);
      return;
    }
    if (!lineageReporterFactoryClassName.isPresent()) {
      LOGGER.warn("Missing the config: {}, skip to enable lineage feature", LineageConfig.LINEAGE_REPORTER_FACTORY);
      return;
    }

    LineageFactory lineageFactory = ReflectionUtil.getObj(lineageFactoryClassName.get(), LineageFactory.class);
    LineageReporterFactory lineageReporterFactory =
        ReflectionUtil.getObj(lineageReporterFactoryClassName.get(), LineageReporterFactory.class);
    LineageReporter lineageReporter = lineageReporterFactory.getLineageReporter(config);

    lineageReporter.start();
    lineageReporter.report(lineageFactory.getLineage(config));
    lineageReporter.stop();

    LOGGER.info("Emitted lineage data to sink stream for job: {}", new ApplicationConfig(config).getAppName());
  }
}
