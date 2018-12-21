package com.cuteximi.spark

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object Score {

  def main(args: Array[String]): Unit = {

    val conf= new SparkConf().setAppName("score").setMaster("local[1]")

    val sc= new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val hiveContext = new HiveContext(sc)


    val score = sc.textFile("src/main/resources/score.txt")

    // row 类型的 RDD
    val rowScore = score.map(_.split(",")).map(item=>Row(item(1),item(2),item(3),item(4),item(5),item(6)))

    // schema
    val schemaString = "id,studentId,language,math,english,classId,departmentId"

    val schema = StructType(schemaString.split(",").map(fieldName=>{StructField(fieldName,StringType,true)}))

    val dataFrame = sqlContext.createDataFrame(rowScore,schema)

    val dataFrame2 = hiveContext.createDataFrame(rowScore,schema)

    dataFrame.registerTempTable("score")
    dataFrame2.registerTempTable("score2")

    dataFrame.show()

    /**
      * 使用开窗函数
      * row_number() OVER (PARTITION BY COL1 ORDER BY COL2) rank
      * 根据COL1分组,在分组内部根据COL2排序,rank：每组内部排序后的编号字段
      * 这里用了两段SQl:
      *  1)(SELECT *, row_number() OVER (PARTITION BY departmentId,classId ORDER BY math DESC) rank FROM scoresTable ) tmp
      *  用开窗函数：按departmentId,classId分组;分组内部按math降序;每组序号rank从1开始;表别名tmp
      *  2)SELECT * FROM  tmp WHERE rank <= 3
      *  保留rank <= 3的数据
      */

    hiveContext.sql("SELECT departmentId,classId,language,studentId FROM (SELECT *, row_number() OVER(PARTITION BY departmentId,classId ORDER BY language DESC) rank FROM score2 ) tmp WHERE rank <= 3").show()


  }

//    /**DataFrame*/
//    import sparkSession.implicits._
//    val scoreInfo = sparkSession.read.textFile(").map(_.split(",")).map(item=>(item(1),item(2).toInt,item(3).toInt,item(4).toInt,item(5),item(6)))
//      .toDF("studentId","language","math","english","classId","departmentId")

//    /**注册DataFrame成一个临时视图*/
//    scoreInfo.createOrReplaceTempView("scoresTable")
//
//
//
//    //语文前3
//    println("############# 语文前3 ##############")
//    sparkSession.sql("
//
//    //数学前3
//    println("############# 数学前3 ##############")
//    sparkSession.sql("SELECT departmentId,classId,math,studentId FROM (SELECT *, row_number() OVER (PARTITION BY departmentId,classId ORDER BY math DESC) rank FROM scoresTable ) tmp WHERE rank <= 3").show()
//
//    //外语前3
//    println("############# 外语前3 ##############")
//    sparkSession.sql("SELECT departmentId,classId,english,studentId FROM (SELECT *, row_number() OVER (PARTITION BY departmentId,classId ORDER BY english DESC) rank FROM scoresTable ) tmp WHERE rank <= 3").show()
  }

