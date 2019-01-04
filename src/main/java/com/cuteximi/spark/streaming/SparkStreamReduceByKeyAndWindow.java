package com.cuteximi.spark.streaming;

import org.apache.spark.SparkConf;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @program: sparktest2
 * @description: 测试窗口函数，以及优化
 * @author: TSL
 * @create: 2018-12-21 16:13
 **/
public class SparkStreamReduceByKeyAndWindow {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Window").setMaster("local[2]");

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.minutes(30));

        JavaDStream DStream  = javaStreamingContext.socketTextStream("node01",8888);

        JavaDStream wordStream = DStream.flatMap(new FlatMapFunction<String,String>() {
            public Iterable<String> call(String s){

                return Arrays.asList(s.split(""));
            }
        });

        JavaPairDStream pairStream = wordStream.mapToPair(new PairFunction<String,String,Integer>() {
            public Tuple2 call(String o){
                return new Tuple2(o,1);
            }
        });





    }
}
