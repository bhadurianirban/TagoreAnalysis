package org.dgrf.TagoreAnalysis

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.io.Source



object TagoreAnalysisMain {
  var inputDir = "/home/dgrfi/MEGA/ML/inputdata"
  var tempDir:String = _
  var outPutDir:String = _
  def main(args: Array[String]): Unit = {
    println("Gheu")
    val sparkSession = SparkSession.builder().appName("bheua").master("local").getOrCreate()

    //val sqlContext = sparkSession.sqlContext
  //ss
    val d = new File(inputDir)
    tempDir = d.getParent+"/temp/"
    outPutDir = d.getParent+"/output/"

    val inputFileList = getListOfFiles(inputDir,".txt")
    inputFileList.foreach(x=> performAnalysis(x.getName,sparkSession))

    splitWCandCC()
  }

  def getListOfFiles(dir: String,extension:String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).filter(f=>f.getName.endsWith(extension)).toList
    } else {
      List[File]()
    }
  }

  def performAnalysis (inputFileName:String,sparkSession:SparkSession): Unit = {
    val fileNameWithoutExtension = inputFileName.replaceAll(".txt","")

    val outTempFilePath = tempDir+fileNameWithoutExtension

    val inputFileWithFullPath = inputDir+File.separator+inputFileName
    //println(outCountFileName+" "+outSentenceFileName+" "+inputFileWithFullPath)
    val ta = new TextAnalyser(sparkSession,inputFileWithFullPath)

    ta.countWords(outTempFilePath)

    mergePartFiles(fileNameWithoutExtension,sparkSession,"WordCount")
    ta.countWordsPerLine(outTempFilePath)

    mergePartFiles(fileNameWithoutExtension,sparkSession,"Sentence")


  }
  def mergePartFiles(fileNameWithoutExtension:String,sparkSession:SparkSession,resultFunction:String): Unit = {
    //use hadoop FileUtil to merge all partition csv files into a single file
    val outTempFilePath = tempDir+fileNameWithoutExtension

    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val resultFilePath = outPutDir+resultFunction+File.separator+fileNameWithoutExtension+".csv"
    val dstPath = new Path(resultFilePath)
    if (fs.exists(dstPath)) {
      fs.delete(dstPath,true)
    }

    FileUtil.copyMerge(fs, new Path(outTempFilePath), fs, dstPath, true, sparkSession.sparkContext.hadoopConfiguration, null)
  }
  
  def splitWCandCC(): Unit = {
    val sentenceDir = outPutDir+File.separator+"Sentence"
    val inputFileList = getListOfFiles(sentenceDir,".csv")
    inputFileList.foreach(file=>writeWC(file))

  }

  def writeWC(sentenceFile:File): Unit = {
    val bufferedSource = Source.fromFile(sentenceFile)
    val sentenceFilePath = sentenceFile.getParent
    val sentenceFileName = sentenceFile.getName.replaceAll(".csv","")

    val charCountFilePath = sentenceFilePath+File.separator+"CC"
    val charCountFileName = sentenceFileName+".txt"
    val charCountFileFullPath = charCountFilePath+File.separator+charCountFileName

    val wordCountFilePath = sentenceFilePath+File.separator+"WC"
    val wordCountFileName = sentenceFileName+".txt"
    val wordCountFileFullPath = wordCountFilePath+File.separator+wordCountFileName

    val charCountFileWriter = new File(charCountFileFullPath)
    val cbw = new BufferedWriter(new FileWriter(charCountFileWriter))

    val wordCountFileWriter = new File(wordCountFileFullPath)
    val wbw = new BufferedWriter(new FileWriter(wordCountFileWriter))

    for (line <- bufferedSource.getLines) {
      val sentenceRow = line.split(",")

      //println (lineCounter+" n "+gheu(0)+" g "+gheu(1))
      wbw.write(sentenceRow(1)+"\n")
      cbw.write(sentenceRow(2)+"\n")

    }
    wbw.close()
    cbw.close()
    bufferedSource.close()
  }
}
