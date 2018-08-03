package org.apache.samza.example;

import java.util.Collections;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.TaskApplicationSpec;
import org.apache.samza.config.Config;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.runtime.ApplicationRuntime;
import org.apache.samza.runtime.ApplicationRuntimes;
import org.apache.samza.storage.kv.RocksDbTableDescriptor;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.util.CommandLine;


/**
 * Created by yipan on 7/16/18.
 */
public class TaskApplicationExample implements TaskApplication {

  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ApplicationRuntime appRuntime = ApplicationRuntimes.getApplicationRuntime(new TaskApplicationExample(), config);
    appRuntime.run();
    appRuntime.waitForFinish();
  }

  @Override
  public void describe(TaskApplicationSpec appBuilder) {
    // add input and output streams
    appBuilder.addInputStreams(Collections.singletonList("myinput"));
    appBuilder.addOutputStreams(Collections.singletonList("myoutput"));
    TableDescriptor td = new RocksDbTableDescriptor("mytable");
    appBuilder.addTables(Collections.singletonList(td));
    // create the task factory based on configuration
    appBuilder.setTaskFactory(TaskFactoryUtil.createTaskFactory(appBuilder.getConfig()));
  }

}
