package org.dgrf.TagoreAnalysis

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object TagoreAnalysisMain {
  def main(args: Array[String]): Unit = {
    println("Gheu")
    val sparkSession = SparkSession.builder().appName("bheua").master("local").getOrCreate()

    //val sqlContext = sparkSession.sqlContext
  //ss
    val inputFileName = "/home/bhaduri/MEGA/ML/Gora.txt"
    val outputFileName = "/home/bhaduri/MEGA/ML/GoraCount"

    val ta = new TextAnalyser(sparkSession,inputFileName,outputFileName)
    ta.countWords("/home/bhaduri/MEGA/ML/GoraCount1")
  }

}
