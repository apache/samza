package org.apache.samza.application;

import org.apache.samza.application.internal.TaskApplicationBuilder;
import org.apache.samza.runtime.internal.TaskApplicationSpec;


/**
 * Created by yipan on 7/11/18.
 */
public interface TaskApplication extends UserApplication<TaskApplication, TaskApplicationBuilder, TaskApplicationSpec> {
}
