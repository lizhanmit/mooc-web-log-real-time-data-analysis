# Spark Streaming Real Project Tutorial

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

**Note:** Remember to add MySql Maven dependency in pom.xml file.

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

Use Spark Streaming to filter blacklist.

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
