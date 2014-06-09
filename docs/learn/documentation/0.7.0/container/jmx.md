---
layout: page
title: JMX
---

Samza's containers and YARN ApplicationMaster enable [JMX](http://docs.oracle.com/javase/tutorial/jmx/) by default. JMX can be used for managing the JVM; for example, you can connect to it using [jconsole](http://docs.oracle.com/javase/7/docs/technotes/guides/management/jconsole.html), which is included in the JDK.

You can tell Samza to publish its internal [metrics](metrics.html), and any custom metrics you define, as JMX MBeans. To enable this, set the following properties in your job configuration:

    # Define a Samza metrics reporter called "jmx", which publishes to JMX
    metrics.reporter.jmx.class=org.apache.samza.metrics.reporter.JmxReporterFactory

    # Use it (if you have multiple reporters defined, separate them with commas)
    metrics.reporters=jmx

JMX needs to be configured to use a specific port, but in a distributed environment, there is no way of knowing in advance which ports are available on the machines running your containers. Therefore Samza chooses the JMX port randomly. If you need to connect to it, you can find the port by looking in the container's logs, which report the JMX server details as follows:

    2014-06-02 21:50:17 JmxServer [INFO] According to InetAddress.getLocalHost.getHostName we are samza-grid-1234.example.com
    2014-06-02 21:50:17 JmxServer [INFO] Started JmxServer registry port=50214 server port=50215 url=service:jmx:rmi://localhost:50215/jndi/rmi://localhost:50214/jmxrmi
    2014-06-02 21:50:17 JmxServer [INFO] If you are tunneling, you might want to try JmxServer registry port=50214 server port=50215 url=service:jmx:rmi://samza-grid-1234.example.com:50215/jndi/rmi://samza-grid-1234.example.com:50214/jmxrmi

## [JobRunner &raquo;](../jobs/job-runner.html)
