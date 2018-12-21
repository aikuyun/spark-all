package com.cuteximi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Int;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;

import java.util.ArrayList;

/**
 * @program: sparktest2
 * @description: SparkStream 测试
 * @author: TSL
 * @create: 2017-12-21 11:48
 **/
public class SparkStreamTest {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        // 注意这里是2
        conf.setAppName("test").setMaster("local[2]");

        // 获取StreamContext
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, new Duration(5000));

        JavaDStream javaDStream = javaStreamingContext.socketTextStream("node01",7777);

        JavaDStream wordDStream = javaDStream.flatMap(new FlatMapFunction<String,String>() {
            public Iterable call(String  s) throws Exception {
                String[] list = s.split(" ");

                return Arrays.asList(list);
            }
        });

        JavaPairDStream pariDStream = wordDStream.mapToPair(new PairFunction<String,String,Integer>() {
            public Tuple2 call(String word) throws Exception {

                return new Tuple2(word,1);
            }
        });

        JavaPairDStream resultDStream = pariDStream.reduceByKey(new Function2<Integer, Integer,Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        resultDStream.print();

        // 开始
        javaStreamingContext.start();

        javaStreamingContext.awaitTermination();

        javaStreamingContext.stop();




    }
}
