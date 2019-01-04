package com.cuteximi.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;


/**
 * @program: sparktest2
 * @description: SparkStream 测试
 * @author: TSL
 * @create: 2017-12-21 11:48
 *
 * 官网上的描述：
 *
 * Data can be ingested from many sources like Kafka, Flume, Twitter, ZeroMQ, Kinesis,
 * or TCP sockets, and can be processed using complex algorithms expressed with high-level functions like map,
 * reduce, join and window. Finally, processed data can be pushed out to filesystems, databases, and live dashboards.
 **/
public class SparkStreamQuickStart {

    /**
     * 操作说明：Java 版
     * 在终端运行 nc -lk 9999 然后输入一些句子，以空格分割
     * 然后运行这个代码。
     * @param args
     */
    public static void main(String[] args) {

        // Create a local StreamingContext with two working thread and batch interval of 1 second
        // 每隔一秒把一批数据封装成 DStream
        SparkConf conf = new SparkConf().setAppName("start").setMaster("local[2]");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(1));

        // Using this context, we can create a DStream that represents streaming data from a TCP source,
        // specified as hostname (e.g. localhost) and port (e.g. 9999). 监听 socket 的数据
        JavaDStream javaDStream = javaStreamingContext.socketTextStream("localhost",9999);

        // Split each line into words 把每一行分割成每一个单词
        JavaDStream wordDStream = javaDStream.flatMap(
                new FlatMapFunction<String,String>() {
                    public Iterable call(String  s){
                        return Arrays.asList(s.split(" "));
            }
        });

        // Count each word in each batch
        JavaPairDStream<String,Integer> pairDStream = wordDStream.mapToPair(
                new PairFunction<String,String,Integer>() {
                    public Tuple2<String,Integer> call(String word){
                        return new Tuple2(word,1);
                    }
        });
        JavaPairDStream<String,Integer> resultDStream = pairDStream.reduceByKey(
                new Function2<Integer, Integer,Integer>() {
                    public Integer call(Integer v1, Integer v2){
                        return v1 + v2;
                    }
        });

        // Print the first ten elements of each RDD generated in this DStream to the console
        resultDStream.print();

        // 开始  finally call start method
        javaStreamingContext.start();

        // Wait for the computation to terminate
        javaStreamingContext.awaitTermination();

        javaStreamingContext.stop(false);


    }
}
