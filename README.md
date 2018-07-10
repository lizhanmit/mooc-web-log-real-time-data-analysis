# Spark Streaming Real Project Tutorial

## src/main/java/com/zhandev/spark/kafka

KafkaProducer.java produces messages and KafkaConsumer.java consumes messages.

Steps:

1. In terminal A, start ZooKeeper. Under `zookeeper/bin`, command line: `zkServer.sh start`.
2. In terminal B, start Kafka. Under `kafka_2.11-0.9.0.0`, command line: `bin/kafka-server-start.sh config/server.properties`.
3. In IDEA, run KafkaClientApp.java.
4. In IDEA console, you should see

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
4. In IDEA console, you should see

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
4. In IDEA console, you should see

```
(a,3)
(b,2)
(c,1)
```

---

