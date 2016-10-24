package org.apache.samza.test.integration;

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.JobRunner;
import org.apache.samza.job.StreamJob;
import org.apache.samza.job.local.ThreadJobFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by jvenkatr on 10/23/16.
 */
public class TestThreadJob {

  public void test() throws Exception {
    Config config = getConfig();
    System.out.println(config);
    JobRunner jobRunner = new JobRunner(config);
    jobRunner.run(true);
  }

  public Config getConfig() throws Exception {
    Properties properties = new Properties();
    properties.load(new FileInputStream(new File("/Users/jvenkatr/code/dummyoutput_INTEG.properties")));
    Map map = new HashMap<>();

    for (final String name: properties.stringPropertyNames())
      map.put(name, properties.getProperty(name));

    return new MapConfig(map);
  }

  public static void main(String[] args) throws Exception {
    TestThreadJob tt = new TestThreadJob();
    tt.test();/*
    long start = System.nanoTime();

    System.out.println(start);
    long startMillis = TimeUnit.NANOSECONDS.toMillis(start);

    long timeoutMs = 10000;
    while(true) {
      Thread.sleep(1000);
      //long currTimeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
      long currTimeMillis = System.currentTimeMillis();

      System.out.println(currTimeMillis + " " + startMillis + " " + (currTimeMillis - startMillis) + " ");
      System.out.println(currTimeMillis - startMillis > timeoutMs);
    }
    */
  }

}
