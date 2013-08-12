---
layout: page
title: JMX
---

The Samza TaskRunner (and YARN Application Master) will turn on JMX using a randomly selected port, since Samza is meant to be run in a distributed environment, and it's unknown which ports will be available prior to runtime. The port will be output in the TaskRunner's logs with a line like this:

    2013-07-05 20:42:36 JmxServer [INFO] According to InetAddress.getLocalHost.getHostName we are Chriss-MacBook-Pro.local
    2013-07-05 20:42:36 JmxServer [INFO] Started JmxServer port=64905 url=service:jmx:rmi:///jndi/rmi://Chriss-MacBook-Pro.local:64905/jmxrmi

Any metrics that are registered in the TaskRunner will be visible through JMX. To toggle JMX, see the [Configuration](../jobs/configuration.html) section.

## [JobRunner &raquo;](../jobs/job-runner.html)
