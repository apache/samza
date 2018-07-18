package org.apache.samza.example;

import java.util.Collections;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.internal.TaskApplicationBuilder;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.ApplicationRuntime;
import org.apache.samza.runtime.ApplicationRuntimes;
import org.apache.samza.task.TaskFactory;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.util.CommandLine;


/**
 * Created by yipan on 7/16/18.
 */
public class TaskApplicationExample implements TaskApplication {

  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ApplicationRuntime appRuntime = ApplicationRuntimes.createTaskApp(new TaskApplicationExample(), config);
    appRuntime.start();
    appRuntime.waitForFinish();
  }

  @Override
  public void init(TaskApplicationBuilder appBuilder, Config config) {
    // add input and output streams
    appBuilder.addInputStreams(Collections.singletonList("myinput"));
    appBuilder.addOutputStreams(Collections.singletonList("myoutput"));
    appBuilder.addTables(Collections.singletonList("mytable"));
    // create the task factory based on configuration
    appBuilder.setTaskFactory((TaskFactory) TaskFactoryUtil.createTaskFactory(config));
  }

}
