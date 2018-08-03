package org.apache.samza.application;

import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.task.TaskFactory;
import org.apache.samza.task.TaskFactoryUtil;


/**
 * Created by yipan on 7/22/18.
 */
public class ApplicationClassUtils {
  public static ApplicationBase fromConfig(Config config) {
    ApplicationConfig appConfig = new ApplicationConfig(config);
    if (appConfig.getAppClass() != null && !appConfig.getAppClass().isEmpty()) {
      try {
        Class<ApplicationBase> appClass = (Class<ApplicationBase>) Class.forName(appConfig.getAppClass());
        if (StreamApplication.class.isAssignableFrom(appClass) || TaskApplication.class.isAssignableFrom(appClass)) {
          return appClass.newInstance();
        }
      } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
        throw new ConfigException(String.format("Loading app.class %s failed. The user application has to implement "
            + "StreamApplication or TaskApplication.", appConfig.getAppClass()), e);
      }
    }
    // no app.class defined. It has to be a legacy application with task.class configuration
    return (TaskApplication) (appSpec) -> appSpec.setTaskFactory((TaskFactory) TaskFactoryUtil.createTaskFactory(config));
  }
}
