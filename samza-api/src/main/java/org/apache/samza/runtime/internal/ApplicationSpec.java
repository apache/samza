package org.apache.samza.runtime.internal;

import org.apache.samza.application.UserApplication;
import org.apache.samza.config.Config;


/**
 * Created by yipan on 7/17/18.
 */
public interface ApplicationSpec<T extends UserApplication> {
  Config getConfig();

  T getUserApp();

  String getGlobalAppId();
}
