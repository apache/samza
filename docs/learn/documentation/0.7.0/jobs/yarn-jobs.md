---
layout: page
title: YARN Jobs
---

When you define job.factory.class=samza.job.yarn.YarnJobFactory in your job's configuration, Samza will use YARN to execute your job. The YarnJobFactory will use the YARN_HOME environment variable on the machine that run-job.sh is executed on to get the appropriate YARN configuration, which will define where the YARN resource manager is. The YarnJob will work with the resource manager to get your job started on the YARN cluster.

If you want to use YARN to run your Samza job, you'll also need to define the location of your Samza job's package. For example, you might say:

```
yarn.package.path=http://my.http.server/jobs/ingraphs-package-0.0.55.tgz
```

This .tgz file follows the conventions outlined on the [Packaging](packaging.html) page (it has bin/run-am.sh and bin/run-container.sh). YARN NodeManagers will take responsibility for downloading this .tgz file on the appropriate machines, and untar'ing them. From there, YARN will execute run-am.sh or run-container.sh for the Samza Application Master, and TaskRunner, respectively.

## [Logging &raquo;](logging.html)
