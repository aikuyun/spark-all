package com.cuteximi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

/**
 * @program: sparktest2
 * @description: 构建 DataFrame
 * @author: TSL
 * @create: 2017-12-19 22:39
 **/
public class CreateDataFrameTest {


    //1. 从 json 文件中创建，直接创建

    //2. 从 json 格式的RDD创建

    //3. 从非 json 的文件转换


    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("p");
        conf.setMaster("local[1]");


        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD rdd = javaSparkContext.textFile("src/main/resources/person.txt");

        SQLContext sqlContext = new SQLContext(javaSparkContext);

        String schemaString = "name age";

        List<StructField> listFields = new ArrayList<StructField>();

        for (String fileName:schemaString.split(" ")){
            listFields.add(DataTypes.createStructField(fileName,DataTypes.StringType,true));
        }

        StructType schema = DataTypes.createStructType(listFields);

        // Convert records of RDD to Rows

        JavaRDD<Row> rowJavaRDD = rdd.map(
            new Function<String,Row>() {
                public Row call(String record) throws Exception {
                    String[] fields = record.split(",");
                    return RowFactory.create(fields[0],fields[1].trim());
                }
        });

        // Apply the schema to Th RDD

        DataFrame personDataFrame = sqlContext.createDataFrame(rowJavaRDD,schema);

        personDataFrame.registerTempTable("person");

        DataFrame results = sqlContext.sql("select * from person");


        results.show();
//        results.javaRDD().map(new Function<Row, String>() {
//            public String call(Row row){
//                return "Name: "+row.getString(0);
//            }
//        }).collect();

    }

    }

