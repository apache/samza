---
layout: page
title: JobRunner
---
<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

Samza jobs are started using a script called run-job.sh.

```
samza-example/target/bin/run-job.sh \
  --config-factory=samza.config.factories.PropertiesConfigFactory \
  --config-path=file://$PWD/config/hello-world.properties
```

You provide two parameters to the run-job.sh script. One is the config location, and the other is a factory class that is used to read your configuration file. The run-job.sh script is actually executing a Samza class called JobRunner. The JobRunner uses your ConfigFactory to get a Config object from the config path.

```
public interface ConfigFactory {
  Config getConfig(URI configUri);
}
```

The Config object is just a wrapper around Map<String, String>, with some nice helper methods. Out of the box, Samza ships with the PropertiesConfigFactory, but developers can implement any kind of ConfigFactory they wish.

Once the JobRunner gets your configuration, it gives your configuration to the StreamJobFactory class defined by the "job.factory" property. Samza ships with two job factory implementations: LocalJobFactory and YarnJobFactory. The StreamJobFactory's responsibility is to give the JobRunner a job that it can run.

```
public interface StreamJob {
  StreamJob submit();

  StreamJob kill();

  ApplicationStatus waitForFinish(long timeoutMs);

  ApplicationStatus waitForStatus(ApplicationStatus status, long timeoutMs);

  ApplicationStatus getStatus();
}
```

Once the JobRunner gets a job, it calls submit() on the job. This method is what tells the StreamJob implementation to start the SamzaContainer. In the case of LocalJobRunner, it uses a run-container.sh script to execute the SamzaContainer in a separate process, which will start one SamzaContainer locally on the machine that you ran run-job.sh on.

This flow differs slightly when you use YARN, but we'll get to that later.

## [Configuration &raquo;](configuration.html)
