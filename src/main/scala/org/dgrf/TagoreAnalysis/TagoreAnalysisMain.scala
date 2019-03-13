package org.dgrf.TagoreAnalysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}

object TagoreAnalysisMain {
  def main(args: Array[String]): Unit = {
    println("Gheu")
    val sparkSession = SparkSession.builder().appName("bheua").master("local").getOrCreate()

    //val sqlContext = sparkSession.sqlContext
  //ss
    val inputfile = "/home/dgrfi/Downloads/Gora.txt"
    val readFileDF = sparkSession.sparkContext.textFile(inputfile)
    val wordsRdd = readFileDF.flatMap(_.split(" ")).map(s=> Row.fromSeq(s))
    wordsRdd.take(10).foreach(println)
    /*val schema = StructType(Array(
      StructField("word", StringType)
    ))
    val wordsDF = sparkSession.createDataFrame(wordsRdd,schema)*/

    /*val wcounts3 = wordsDF.filter(r => (r(0) =="Humpty") || (r(0) == "Dumpty"))
      .groupBy("Value")
      .count()
    wcounts3.collect.foreach(println)*/

  }

}
