package org.apache.samza.runtime.internal;

import java.util.Collection;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.operators.ContextManager;


/**
 * Created by yipan on 7/17/18.
 */
public interface StreamApplicationSpec extends ApplicationSpec<StreamApplication> {

  ContextManager getContextManager();

  Collection<String> getInputStreams();

  Collection<String> getOutputStreams();

  Collection<String> getBroadcastStreams();

  Collection<String> getTables();



}
