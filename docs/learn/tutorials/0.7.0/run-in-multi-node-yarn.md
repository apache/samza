---
layout: page
title: Run Hello-samza in Multi-node YARN
---

You must successfully run the [hello-samza](../../../startup/hello-samza/0.7.0/) project in a single-node YARN by following the [hello-samza](../../../startup/hello-samza/0.7.0/) tutorial. Now it's time to run the Samza job in a "real" YARN grid (with more than one node).

## Set Up Multi-node YARN

If you already have a multi-node YARN cluster (such as CDH5 cluster), you can skip this set-up section.

### Basic YARN Setting

1\. Dowload [YARN 2.3](http://mirror.symnds.com/software/Apache/hadoop/common/hadoop-2.3.0/hadoop-2.3.0.tar.gz) to /tmp and untar it.

```
cd /tmp
tar -xvf hadoop-2.3.0.tar.gz
cd hadoop-2.3.0
```

2\. Set up environment variables.

```
export HADOOP_YARN_HOME=$(pwd)
mkdir conf
export HADOOP_CONF_DIR=$HADOOP_YARN_HOME/conf
```

3\. Configure YARN setting file.

```
cp ./etc/hadoop/yarn-site.xml conf
vi conf/yarn-site.xml
```

Add the following property to yarn-site.xml:

```
<property>
    <name>yarn.resourcemanager.hostname</name>
    <!-- hostname that is accessible from all NMs -->
    <value>yourHostname</value>
</property>
```

Download and add capacity-schedule.xml.

```
curl http://svn.apache.org/viewvc/hadoop/common/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-tests/src/test/resources/capacity-scheduler.xml?view=co > conf/capacity-scheduler.xml
```

###Set Up Http Filesystem for YARN

The goal of these steps is to configure YARN to read http filesystem because we will use Http server to deploy Samza job package. If you want to use HDFS to deploy Samza job package, you can skip step 4~6 and follow [Deploying a Samza Job from HDFS](deploy-samza-job-from-hdfs.html)

4\. Download Scala package and untar it.

```
cd /tmp
curl http://www.scala-lang.org/files/archive/scala-2.10.3.tgz > scala-2.10.3.tgz
tar -xvf scala-2.10.3.tgz
```

5\. Add Scala and its log jars.

```
cp /tmp/scala-2.10.3/lib/scala-compiler.jar $HADOOP_YARN_HOME/share/hadoop/hdfs/lib
cp /tmp/scala-2.10.3/lib/scala-library.jar $HADOOP_YARN_HOME/share/hadoop/hdfs/lib
curl http://search.maven.org/remotecontent?filepath=org/clapper/grizzled-slf4j_2.10/1.0.1/grizzled-slf4j_2.10-1.0.1.jar > $HADOOP_YARN_HOME/share/hadoop/hdfs/lib/grizzled-slf4j_2.10-1.0.1.jar
```

6\. Add http configuration in core-site.xml (create the core-site.xml file and add content).

```
vi $HADOOP_YARN_HOME/conf/core-site.xml
```

Add the following code:

```
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
      <name>fs.http.impl</name>
      <value>org.apache.samza.util.hadoop.HttpFileSystem</value>
    </property>
</configuration>
```

### Distribute Hadoop File to Slaves

7\. Basically, you copy the hadoop file in your host machine to slave machines. (172.21.100.35, in my case):

```
scp -r . 172.21.100.35:/tmp/hadoop-2.3.0
echo 172.21.100.35 > conf/slaves
sbin/start-yarn.sh
```

* If you get "172.21.100.35: Error: JAVA_HOME is not set and could not be found.", you'll need to add a conf/hadoop-env.sh file to the machine with the failure (172.21.100.35, in this case), which has "export JAVA_HOME=/export/apps/jdk/JDK-1_6_0_27" (or wherever your JAVA_HOME actually is).

8\. Validate that your nodes are up by visiting http://yourHostname:8088/cluster/nodes.

## Deploy Samza Job

Some of the following steps are exactlly identical to what you have seen in [hello-samza](../../../startup/hello-samza/0.7.0/). You may skip them if you have already done so.

1\. Download Samza and publish it to Maven local repository.

```
cd /tmp
git clone http://git-wip-us.apache.org/repos/asf/incubator-samza.git
cd incubator-samza
./gradlew clean publishToMavenLocal
cd ..
```

2\. Download hello-samza project and change the job properties file.

```
git clone git://github.com/linkedin/hello-samza.git
cd hello-samza
vi samza-job-package/src/main/config/wikipedia-feed.properties
```

Change the yarn.package.path property to be:

```
yarn.package.path=http://yourHostname:8000/samza-job-package/target/samza-job-package-0.7.0-dist.tar.gz
```

3\. Complie hello-samza.

```
mvn clean package
mkdir -p deploy/samza
tar -xvf ./samza-job-package/target/samza-job-package-0.7.0-dist.tar.gz -C deploy/samza
```

4\. Deploy Samza job package to Http server..

Open a new terminal, and run:

```
cd /tmp/hello-samza && python -m SimpleHTTPServer
```

Go back to the original terminal (not the one running the HTTP server):

```
deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-feed.properties
```

Go to http://yourHostname:8088 and find the wikipedia-feed job. Click on the ApplicationMaster link to see that it's running.

Congratulations! You now run the Samza job in a "real" YARN grid!

