package org.dgrf.TagoreAnalysis

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class TextAnalyser () {
  private var sparkSession:SparkSession = _
  private var inputFileName:String = _
  private var outputFileName:String = _
  def this (sparkSession:SparkSession,inputFileName:String,outputFileName:String) {
    this()
    this.sparkSession = sparkSession
    this.inputFileName = inputFileName
    this.outputFileName = outputFileName
  }
  def countWords (outputFileName:String = this.outputFileName): Unit = {


    val readFileRdd = sparkSession.sparkContext.textFile(inputFileName)
    val readFileFormatted =
      readFileRdd
        .map(s => s.replaceAll("[-]"," "))
        .map(s => s.replaceAll("[,।;?!\"]",""))
        .map(s => s.replaceAll("[\\s+]"," "))
        .map(s=>s.trim)
        .filter(x => !x.isEmpty)
    readFileFormatted.take(10).foreach(println)
    val wordsRdd = readFileFormatted.flatMap(_.split("\\s+")).map(s=> Row(s))
    //wordsRdd.take(100).foreach(println)
    val schema = StructType(Array(
      StructField("word", StringType)
    ))
    val wordsDF = sparkSession.createDataFrame(wordsRdd,schema)
    //wordsDF.show(10)

    val wcounts3 = wordsDF
      .groupBy("word")
      .count()
    wcounts3.write.mode(SaveMode.Overwrite).csv(outputFileName)
  }
  def countWordsPerLine (outputFileName:String = this.outputFileName): Unit = {
    sparkSession.conf.set("textinputformat.record.delimiter",",")
    val readFileRdd = sparkSession.sparkContext.textFile(inputFileName)
   /* val readFileFormatted =
      readFileRdd
        .map(s => s.replaceAll("[-]"," "))
        .map(s => s.replaceAll("[,;\"]",""))
        .map(s => s.replaceAll("[\\s+]"," "))
        .map(s=>s.trim)
        .filter(x => !x.isEmpty)*/
    readFileRdd.take(10).foreach(println)
  }
}
