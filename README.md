# Spark 

spark 代码练习S，结合官网实例和项目的例子。

## SparkStreaming 

> 保留官网最原汁原味的说明，有时候发现中文的表达太晦涩了，还是英文比较直接一点。

Spark Streaming is an extension of the core Spark API that enables scalable, high-throughput, fault-tolerant stream processing of live data streams. Data can be ingested from many sources like Kafka, Flume, Twitter, ZeroMQ, Kinesis, or TCP sockets, and can be processed using complex algorithms expressed with high-level functions like map, reduce, join and window. Finally, processed data can be pushed out to filesystems, databases, and live dashboards.
In fact, you can apply Spark’s machine learning and graph processing algorithms on data streams.

[数据源和数据流向](https://github.com/aikuyun/spark-all/blob/master/src/image/streaming-arch.png)

Internally, it works as follows. Spark Streaming receives live input data streams and divides the data into batches, which are then processed by the Spark engine to generate the final stream of results in batches.

[DStream](https://github.com/aikuyun/spark-all/blob/master/src/image/streaming-flow.png)

Spark Streaming provides a high-level abstraction called discretized stream or DStream, which represents a continuous stream of data. DStreams can be created either from input data streams from sources such as Kafka, Flume, and Kinesis, or by applying high-level operations on other DStreams. Internally, a DStream is represented as a sequence of RDDs.

如下图所示，简要的数据处理过程：

[process](https://github.com/aikuyun/spark-all/blob/master/src/image/sparkstreaming01.png)

SparkStream 会启动 receive task 不间断的接收数据，每隔 batchInterval 时间间隔, 变成一个 batch 数据，然后再将 batch 转换成一个 RDD，最后将 RDD 封装成
一个 DStream 。然后就对这个 DStream 使用各种转换算子（懒加载的），以及使用 output operator 算子进行操作。

> 注意需要根据业务调整这个 batchInterval,否则会造成数据的堆积,进而导致 OOM。

### 代码练习

- [QuickStart Java 版 ](https://github.com/aikuyun/spark-all/blob/master/src/main/java/com/cuteximi/spark/streaming/SparkStreamQuickStart.java)

- [QuickStart Scala 版 ](https://github.com/aikuyun/spark-all/blob/master/src/main/scala/com/cuteximi/spark/streaming/StreamingQuickStart.scala)

> 后面为了熟练里面的细节会使用 Java 进行练习。

