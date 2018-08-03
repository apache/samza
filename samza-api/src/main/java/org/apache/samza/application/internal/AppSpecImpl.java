package org.apache.samza.application.internal;

import org.apache.samza.application.ApplicationSpec;
import org.apache.samza.application.ApplicationBase;
import org.apache.samza.application.ProcessorLifecycleListener;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.task.TaskContext;


/**
 * Created by yipan on 7/10/18.
 */
public abstract class AppSpecImpl<T extends ApplicationBase, S extends ApplicationSpec<T>> implements ApplicationSpec<T> {

  final Config config;

  // Default to no-op functions in ContextManager
  ContextManager contextManager = new ContextManager() {
    @Override
    public void init(Config config, TaskContext context) {
    }

    @Override
    public void close() {
    }
  };

  // Default to no-op functions in ProcessorLifecycleListener
  ProcessorLifecycleListener listener = new ProcessorLifecycleListener() {
  };

  protected AppSpecImpl(Config config) {
    this.config = config;
  }

  public static class AppConfig extends MapConfig {

    public static final String APP_NAME = "app.name";
    public static final String APP_ID = "app.id";
    public static final String APP_CLASS = "app.class";

    public static final String JOB_NAME = "job.name";
    public static final String JOB_ID = "job.id";

    public AppConfig(Config config) {
      super(config);
    }

    public String getAppName() {
      return get(APP_NAME, get(JOB_NAME));
    }

    public String getAppId() {
      return get(APP_ID, get(JOB_ID, "1"));
    }

    public String getAppClass() {
      return get(APP_CLASS, null);
    }

    /**
     * Returns full application id
     *
     * @return full app id
     */
    public String getGlobalAppId() {
      return String.format("app-%s-%s", getAppName(), getAppId());
    }

  }

  @Override
  public String getGlobalAppId() {
    return new AppConfig(config).getGlobalAppId();
  }

  @Override
  public Config getConfig() {
    return config;
  }

  @Override
  public S withContextManager(ContextManager contextManager) {
    this.contextManager = contextManager;
    return (S) this;
  }

  @Override
  public S withProcessorLifecycleListener(ProcessorLifecycleListener listener) {
    this.listener = listener;
    return (S) this;
  }

  public ContextManager getContextManager() {
    return contextManager;
  }

  public ProcessorLifecycleListener getProcessorLifecycleListner() {
    return listener;
  }

}