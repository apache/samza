package org.apache.samza.application;

import org.apache.samza.config.Config;


/**
 * Created by yipan on 8/31/17.
 */
public interface ProcessorContextFactory {
  ProcessorContext getProcessorContext(String processorId, Config processorConfig);
}
