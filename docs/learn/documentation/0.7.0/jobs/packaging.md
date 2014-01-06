---
layout: page
title: Packaging
---

The [JobRunner](job-runner.html) page talks about run-job.sh, and how it's used to start a job either locally (LocalJobFactory) or with YARN (YarnJobFactory). In the diagram that shows the execution flow, it also shows a run-container.sh script. This script, along with a run-am.sh script, are what Samza actually calls to execute its code.

```
bin/run-am.sh
bin/run-container.sh
```

The run-container.sh script is responsible for starting the TaskRunner. The run-am.sh script is responsible for starting Samza's application master for YARN. Thus, the run-am.sh script is only used by the YarnJob, but both YarnJob and ProcessJob use run-container.sh.

Typically, these two scripts are bundled into a tar.gz file that has a structure like this:

```
bin/run-am.sh
bin/run-class.sh
bin/run-job.sh
bin/run-container.sh
lib/*.jar
```

To run a Samza job, you un-zip its tar.gz file, and execute the run-job.sh script, as defined in the JobRunner section. There are a number of interesting implications from this packaging scheme. First, you'll notice that there is no configuration in the package. Second, you'll notice that the lib directory contains all JARs that you'll need to run your Samza job.

The reason that configuration is decoupled from your Samza job packaging is that it allows configuration to be updated without having to re-build the entire Samza package. This makes life easier for everyone when you just need to tweak one parameter, and don't want to have to worry about which branch your package was built from, or whether trunk is in a stable state. It also has the added benefit of forcing configuration to be fully resolved at runtime. This means that that the configuration for a job is resolved at the time run-job.sh is called (using --config-path and --config-provider parameters), and from that point on, the configuration is immutable, and passed where it needs to be by Samza (and YARN, if you're using it).

The second statement, that your Samza package contains all JARs that it needs to run, means that a Samza package is entirely self contained. This allows Samza jobs to run on independent Samza versions without conflicting with each other. This is in contrast to Hadoop, where JARs are pulled in from the local machine that the job is running on (using environment variables). With Samza, you might run your job on version 0.7.0, and someone else might run their job on version 0.8.0. There is no problem with this.

## [YARN Jobs &raquo;](yarn-jobs.html)
