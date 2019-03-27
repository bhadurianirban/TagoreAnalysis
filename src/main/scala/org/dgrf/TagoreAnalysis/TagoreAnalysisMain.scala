package org.dgrf.TagoreAnalysis

import java.io.File

import org.apache.hadoop.fs.{FileSystem, FileUtil,Path}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}



object TagoreAnalysisMain {
  var inputDir = "/home/bhaduri/MEGA/ML/inputdata"
  var tempDir:String = _
  var outPutDir:String = _
  def main(args: Array[String]): Unit = {
    println("Gheu")
    val sparkSession = SparkSession.builder().appName("bheua").master("local").getOrCreate()

    //val sqlContext = sparkSession.sqlContext
  //ss
    val d = new File(inputDir)
    tempDir = d.getParent+"/temp/"


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

    val outCountFileName = tempDir+fileName+"Count"
    val outSentenceFileName = tempDir+fileName+"Sentence"
    println(outCountFileName+" "+outSentenceFileName)
    /*val ta = new TextAnalyser(sparkSession,f.getAbsolutePath)
    ta.countWords(outCountFileName)
    ta.countWordsPerLine(outSentenceFileName)
    mergePartFiles(outCountFileName,sparkSession)*/


  }
  def mergePartFiles(outCountFileName:String,sparkSession:SparkSession): Unit = {
    //use hadoop FileUtil to merge all partition csv files into a single file

    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    FileUtil.copyMerge(fs, new Path(outCountFileName), fs, new Path("outout/"+outCountFileName+"target.csv"), true, sparkSession.sparkContext.hadoopConfiguration, null)
  }
  
  

}
