---
layout: page
title: Isolation
---

When running Samza jobs in a shared, distributed environment, the stream processors can have an impact on one another's performance. A stream processor that uses 100% of a machine's CPU will slow down all other stream processors on the machine.

One of [YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html)'s responsibilities is to manage resources so that this doesn't happen. Each of YARN's Node Managers (NM) has a chunk of "resources" dedicated to it. The YARN Resource Manager (RM) will only allow a container to be allocated on a NM if it has enough resources to satisfy the container's needs.

YARN currently supports resource management for memory and CPU.

### Memory

YARN will automatically enforce memory limits for all containers that it executes. All containers must have a max-memory size defined when they're created. If the sum of all memory usage for processes associated with a single YARN container exceeds this maximum, YARN will kill the container.

Samza supports memory limits using the yarn.container.memory.mb and yarn.am.container.memory.mb configuration parameters. Keep in mind that this is simply the amount of memory YARN will allow a [SamzaContainer](../container/samza-container.html) or [ApplicationMaster](application-master.html) to have. You'll still need to configure your heap settings appropriately using task.opts, when using Java (the default is -Xmx160M). See the [Configuration](../jobs/configuration.html) and [Packaging](../jobs/packaging.html) pages for details.

### CPU

YARN has the concept of a virtual core. Each NM is assigned a total number of virtual cores (32, by default). When a container request is made, it must specify how many virtual cores it needs. The YARN RM will only assign the container to a NM that has enough virtual cores to satisfy the request.

#### CGroups

Unlike memory, which YARN can enforce itself (by looking at the /proc folder), YARN can't enforce CPU isolation, since this must be done at the Linux kernel level. One of YARN's interesting new features is its support for Linux [CGroups](https://www.kernel.org/doc/Documentation/cgroups/cgroups.txt). CGroups are a way to control process utilization at the kernel level in Linux.

If YARN is setup to use CGroups, then YARN will guarantee that a container will get at least the amount of CPU that it requires. Currently, YARN will give you more CPU, if it's available. For details on enforcing "at most" CPU usage, see [YARN-810](https://issues.apache.org/jira/browse/YARN-810). 

See [this blog post](http://riccomini.name/posts/hadoop/2013-06-14-yarn-with-cgroups/) for details on setting up YARN with CGroups.

## [Security &raquo;](../operations/security.html)
