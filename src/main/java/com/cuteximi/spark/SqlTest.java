package com.cuteximi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * @program: sparktest2
 * @description: spark sql
 * @author: TSL
 * @create: 2017-12-19 21:38
 **/

public class SqlTest {

    public static void main(String[] args) {

        // 配置
        SparkConf con = new SparkConf();
        con.setAppName("slq");
        con.setMaster("local[1]");

        // 上下文
        JavaSparkContext javaSparkContext = new JavaSparkContext(con);


        SQLContext sqlContext = new SQLContext(javaSparkContext);


        DataFrame dt = sqlContext.read().json("src/main/resources/person.json");

        dt.show(); // 此时列名是按照 asc

        dt.printSchema();

        // 下面是 dt 操作 API

        // 更多 API 操作可以查看 http://spark.apache.org/docs/1.6.3/api/java/org/apache/spark/sql/DataFrame.html

        dt.select("name").show();

        dt.select(dt.col("name"),dt.col("age").plus(1)).show();

        dt.groupBy("age").count().show();

        // 那么下面，将开始使用 SQL

        sqlContext.registerDataFrameAsTable(dt,"person");

        DataFrame df = sqlContext.sql("select name,age from person");

        df.show();

    }
}
