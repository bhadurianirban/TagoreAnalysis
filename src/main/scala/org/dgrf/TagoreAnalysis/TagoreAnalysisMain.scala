package org.dgrf.TagoreAnalysis

import java.io.File

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object TagoreAnalysisMain {
  def main(args: Array[String]): Unit = {
    println("Gheu")
    val sparkSession = SparkSession.builder().appName("bheua").master("local").getOrCreate()

    //val sqlContext = sparkSession.sqlContext
  //ss
    val inputDir = "/home/bhaduri/MEGA/ML"
    val outputFileName = "/home/bhaduri/MEGA/ML/DurgeshnandiniCount"

    val inputFileList = getListOfFiles(inputDir)
    inputFileList.foreach(x=> performAnalysis(x,sparkSession))

    //val outputFileName = "/home/bhaduri/MEGA/ML/DurgeshnandiniSentence"

    /*val ta = new TextAnalyser(sparkSession,inputFileName,outputFileName)
    ta.countWords()*/
  }

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).filter(f=>f.getName.contains("txt")).toList
    } else {
      List[File]()
    }
  }

  def performAnalysis (f:File,sparkSession:SparkSession): Unit = {
    val fileName = f.getName.replaceAll(".txt","")
    val outCountFileName = fileName+"Count"
    val outSentenceFileName = fileName+"Sentence"
    println(outCountFileName+" "+outSentenceFileName)
    val ta = new TextAnalyser(sparkSession,f.getAbsolutePath)
    ta.countWords(outCountFileName)
    ta.countWordsPerLine(outSentenceFileName)

  }

}
