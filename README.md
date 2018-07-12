# Spark Streaming Real Project Tutorial

Version:

- Spark: 2.3.0
- Scala: 2.11.8
- Kafka: 0.9.0.0
- Flume: 1.7.0

---

## src/main/java/com/zhandev/spark/kafka

KafkaProducer.java produces messages and KafkaConsumer.java consumes messages.

Steps:

1. In terminal A, start ZooKeeper. Under `zookeeper/bin`, command line: `zkServer.sh start`.
2. In terminal B, start Kafka. Under `kafka_2.11-0.9.0.0`, command line: `bin/kafka-server-start.sh config/server.properties`.
3. In IDEA, run KafkaClientApp.java.
4. In IDEA console, you will see

```
Sent: message_1
Sent: message_2
receive: message_1
receive: message_2
...
```

---

## src/main/scala/com/zhandev/spark

### NetworkWordCount.scala

Spark Streaming receives socket data and does word count.

Steps:

1. In terminal A, `nc -lk 6789`.
2. In IDEA, run NetworkWordCount.scala.
3. In terminal A, type `a a a b b b b c c`.
4. In IDEA console, you will see

```
(b,4)
(a,3)
(c,2)
```

**Note:** There might be "NoSuchMethodError" exceptions. Just add corresponding Maven dependencies to solve the problem.

![spark-streaming-processes-socket-data-architecture.png](src/main/resources/static/img/spark-streaming-processes-socket-data-architecture.png)

### FileWordCount.scala

Spark Streaming processes file system (local/hdfs) data and does word count.

Steps:

1. In IDEA, run NetworkWordCount.scala.
2. Type `a a a b b c` in file1.log file. Save.
3. Copy file1.log file to `/home/hadoop/IdeaProjects/sparktrain/src/main/resources/static/file`.
4. In IDEA console, you will see

```
(a,3)
(b,2)
(c,1)
```

### StatefulWordCount.scala

Spark Streaming processes socket data with state and does word count. The count will be accumulated based on previous result.

**Note:** If you use stateful operation，you must set checkpoint. In real projects, you should set checkpoint directory on HDFS.

Steps:

1. In terminal A, `nc -lk 6789`.
2. In IDEA, run StatefulWordCount.scala.
3. In terminal A, type `a a a b b b b c c`.
4. In IDEA console, you will see

```
(b,4)
(a,3)
(c,2)
```

5. In terminal A, type `a a b c c c`.
6. In IDEA console, you will see

```
(b,5)
(a,5)
(c,5)
```

### ForeachRDDApp.scala

Spark Streaming processes socket data and save the wordcount result into MySQL.

**Note:** Remember to add MySQL Maven dependency in pom.xml file.

Steps:

1. In terminal A, start MySQL, `mysql -u root -p`. Enter password.
2. Create a database, `create database spark;`.
3. Use this database, `use spark`.
4. Create a table, `create table wordcount (word varchar(50) default null, count int(10) default null);`.
5. In terminal B, `nc -lk 6789`.
6. In IDEA, run ForeachRDDApp.scala.
7. In terminal B, type `a a b c c c`.
8. In IDEA console, you will see

```
(b,1)
(a,2)
(c,3)
```

9. In terminal A, `select * from wordcount`. You will see

```
| b | 1 |
| a | 2 |
| c | 3 |
```

![spark-streaming-save-wordcount-result-into-mysql.png](src/main/resources/static/img/spark-streaming-save-wordcount-result-into-mysql.png)

### BlacklistFiltering.scala

Use Spark Streaming to filter records of log data which are in blacklist.

Steps:

1. In terminal A, `nc -lk 6789`.
2. In IDEA, run BlacklistFiltering.scala.
3. In terminal A, type

```
20180101,zs
20180101,ls
20180101,ww
20180101,zl
```

4. In IDEA console, you will see the records with "zs" and "ls" have been filtered out.

```
20180101,ww
20180101,zl
```

![blacklist-filtering-idea.png](src/main/resources/static/img/blacklist-filtering-idea.png)

### SparkSqlNetworkWordCount.scala

Integrate Spark Streaming and Spark SQL to process socket data and do word count. Convert RDDs of the words DStream to DataFrame and run SQL query.

**Note:** Remember to add spark-sql Maven dependency in pom.xml file.

Steps:

1. In terminal A, `nc -lk 6789`.
2. In IDEA, run SparkSqlNetworkWordCount.scala.
3. In terminal A, type `a a a b b b c c`.
4. In IDEA console, you will see

```
+----+-----+
|word|total|
+----+-----+
|   c|    2|
|   b|    3|
|   a|    3|
+----+-----+
```

### FlumePushWordCount.scala

Integrate Spark Streaming and Flume to process socket data and do word count in push-based approach.

**Note:**

- Remember to add spark-streaming-flume Maven dependency in pom.xml file.
- There are a bit differences between the flume config file and FlumePushWordCount file of local mode and server mode (in real projects).

#### local mode

Steps:

1. Create Flume config file (flume-push-streaming.conf).
2. In IDEA, run FlumePushWordCount.scala. Edit configurations -> Program arguments, input `10.0.2.15 41414`. -> Apply
3. In terminal A, start Flume.

```
flume-ng agent \
--name netcat-memory-avro \
--conf $FLUME_HOME/conf \
--conf-file /home/hadoop/IdeaProjects/sparktrain/src/main/resources/static/flume/flume-push-streaming.conf \
-Dflume.root.logger=INFO,console
```

4. In terminal B, `telnet localhost 44444`. Type `a a b b`.
5. In IDEA console, you will see

```
(a,2)
(b,2)
```

#### server mode (in real projects)

Steps:

1. In terminal A, pack the spark project using maven, under the spark project directory, `mvn clean package -DskipTests` (skip test). Then the .jar file will be created under "target" folder.
2. In terminal B, run the .jar file using spark-submit. (need network to download packages)

```
spark-submit \
--class com.zhandev.spark.FlumePushWordCount \
--master local[2] \
--packages org.apache.spark:spark-streaming-flume_2.11:2.3.0 \
/home/hadoop/IdeaProjects/sparktrain/target/spark-train-1.0.jar \
localhost 41414
```

3. In terminal C, start Flume.

```
flume-ng agent \
--name netcat-memory-avro \
--conf $FLUME_HOME/conf \
--conf-file /home/hadoop/IdeaProjects/sparktrain/src/main/resources/static/flume/flume-push-streaming.conf \
-Dflume.root.logger=INFO,console
```

4. In terminal D, `telnet localhost 44444`. Type `a a b b`.
5. In terminal B, you will see

```
(a,2)
(b,2)
```

### FlumePullWordCount.scala

Integrate Spark Streaming and Flume to process socket data and do word count in pull-based approach. Pull-based approach is better than push-based approach. Use pull-based one in real projects.

**Note:**

- Remember to add spark-streaming-flume-sink, scala-library, and commons-lang3 Maven dependencies in pom.xml file.
- There are a bit differences between the flume config file and FlumePushWordCount file of local mode and server mode (in real projects).
- Start Flume first, then start Spark Streaming.
- Remember to add spark-streaming-flume-sink_2.11-2.3.0.jar, scala-library-2.11.8.jar and commons-lang3-3.5.jar under flume/lib directory. Otherwise, you will get the following error.
- Pay attention to the version of .jar files.

```
org.apache.flume.FlumeException: Unable to load sink type: org.apache.spark.streaming.flume.sink.SparkSink, class: org.apache.spark.streaming.flume.sink.SparkSink
        at org.apache.flume.sink.DefaultSinkFactory.getClass(DefaultSinkFactory.java:71)
        at org.apache.flume.sink.DefaultSinkFactory.create(DefaultSinkFactory.java:43)
        at org.apache.flume.node.AbstractConfigurationProvider.loadSinks(AbstractConfigurationProvider.java:410)
        at org.apache.flume.node.AbstractConfigurationProvider.getConfiguration(AbstractConfigurationProvider.java:98)
        at org.apache.flume.node.PollingPropertiesFileConfigurationProvider$FileWatcherRunnable.run(PollingPropertiesFileConfigurationProvider.java:140)
        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:471)
        at java.util.concurrent.FutureTask.runAndReset(FutureTask.java:304)
        at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$301(ScheduledThreadPoolExecutor.java:178)
        at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
        at java.lang.Thread.run(Thread.java:744)
Caused by: java.lang.ClassNotFoundException: org.apache.spark.streaming.flume.sink.SparkSink
        at java.net.URLClassLoader$1.run(URLClassLoader.java:366)
        at java.net.URLClassLoader$1.run(URLClassLoader.java:355)
        at java.security.AccessController.doPrivileged(Native Method)
        at java.net.URLClassLoader.findClass(URLClassLoader.java:354)
        at java.lang.ClassLoader.loadClass(ClassLoader.java:425)
        at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:308)
        at java.lang.ClassLoader.loadClass(ClassLoader.java:358)
        at java.lang.Class.forName0(Native Method)
        at java.lang.Class.forName(Class.java:190)
        at org.apache.flume.sink.DefaultSinkFactory.getClass(DefaultSinkFactory.java:69)
        ... 11 more
```

#### server mode (in real projects)

Steps:

1. In terminal A, pack the spark project using maven, under the spark project directory, `mvn clean package -DskipTests` (skip test). Then the .jar file will be created under "target" folder.
2. In terminal B, start Flume.

```
flume-ng agent \
--name netcat-memory-spark \
--conf $FLUME_HOME/conf \
--conf-file /home/hadoop/IdeaProjects/sparktrain/src/main/resources/static/flume/flume-pull-streaming.conf \
-Dflume.root.logger=INFO,console
```

3. In terminal C, `telnet localhost 44444`.
4. In terminal D, run the .jar file using spark-submit. (need network to download packages)

```
spark-submit \
--class com.zhandev.spark.FlumePullWordCount \
--master local[2] \
--packages org.apache.spark:spark-streaming-flume_2.11:2.3.0 \
/home/hadoop/IdeaProjects/sparktrain/target/spark-train-1.0.jar \
localhost 41414
```

5. In terminal C, type `a a b b`.
6. In terminal D, you will see

```
(a,2)
(b,2)
```

