package org.apache.samza.coordinator;

import org.apache.samza.config.Config;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.util.*;


public abstract class CoordinationUtilsFactoryAbstract implements CoordinationUtilsFactory {

  public static CoordinationUtilsFactory getCoordinationUtilsFactory(Config config) {
    // load the class
    JobCoordinatorConfig jcConfig = new JobCoordinatorConfig(config);
    String coordinationUtilsFactoryClass =   jcConfig.getJobCoordinationUtilsFactoryClassName();

    return ClassLoaderHelper.fromClassName(coordinationUtilsFactoryClass, CoordinationUtilsFactory.class);
  }
}
