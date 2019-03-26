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
    val inputDir = "/home/dgrfi/MEGA/ML"


    val inputFileList = getListOfFiles(inputDir,".txt")
    inputFileList.foreach(x=> performAnalysis(x,sparkSession))

    //val outputFileName = "/home/bhaduri/MEGA/ML/DurgeshnandiniSentence"

    /*val ta = new TextAnalyser(sparkSession,inputFileName,outputFileName)
    ta.countWords()*/
  }

  def getListOfFiles(dir: String,extension:String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).filter(f=>f.getName.endsWith(extension)).toList
    } else {
      List[File]()
    }
  }

  def performAnalysis (f:File,sparkSession:SparkSession): Unit = {
    val fileName = f.getName.replaceAll(".txt","")
    val outputFileDirectory = f.getParent+"/output/"
    val outCountFileName = outputFileDirectory+fileName+"Count"
    val outSentenceFileName = outputFileDirectory+fileName+"Sentence"
    //println(outCountFileName+" "+outSentenceFileName+" "+outputFileDirectory)
    val ta = new TextAnalyser(sparkSession,f.getAbsolutePath)
    ta.countWords(outCountFileName)
    ta.countWordsPerLine(outSentenceFileName)

    val wordCountFileList = getListOfFiles(outCountFileName,".csv")
    val sentenceFileList = getListOfFiles(outSentenceFileName,".csv")
    wordCountFileList.foreach(println)
    sentenceFileList.foreach(println)

  }
  
  

}
