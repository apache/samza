package org.apache.samza.application;

import org.apache.samza.config.Config;
import org.apache.samza.container.SamzaContainerContext;


/**
 * Created by yipan on 8/31/17.
 */
public interface ProcessorContext {
  SamzaContainerContext getContainerContext();
  TaskContextFactory getTaskContextFactory(String taskId, Config config);
}
