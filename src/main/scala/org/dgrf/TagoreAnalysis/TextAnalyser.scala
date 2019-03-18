package org.dgrf.TagoreAnalysis

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


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

    val readFileRdd = sparkSession.sparkContext.wholeTextFiles(inputFileName).map(s=>s._2)
    val readFileFormatted =
      readFileRdd
        .map(s => s.replaceAll("[-]"," "))
        .map(s => s.replaceAll("[,;\"০১২৩৪৫৬৭৮৯]",""))
        .map(s => s.replaceAll("\\s+"," "))
        .map(s=>s.trim)
        .filter(x => !x.isEmpty)
    val fullText = readFileFormatted.first()
    val sentences = fullText.split("[।?!]").map(s=>s.trim)
    val sentencesArray = sentences.zipWithIndex.map(m=>{
      val sentenceRow = Row(m._2,m._1)
      sentenceRow
    })

    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("sentence", StringType)
    ))
    val sentencesDF = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(sentencesArray),schema)
    sentencesDF.take(10).foreach(row=>countWordsPerSentence(row))

    //foreach(_.split("[।?!]"))
   /* val readFileFormatted =
      readFileRdd
        .map(s => s.replaceAll("[-]"," "))
        .map(s => s.replaceAll("[,;\"]",""))
        .map(s => s.replaceAll("[\\s+]"," "))
        .map(s=>s.trim)
        .filter(x => !x.isEmpty)*/
    //sentenceRdd.take(10).foreach(println)
  }
  private def countWordsPerSentence (row:Row): Unit = {
    val sentenceId = row.getInt(0)
    val sentence = row.getString(1)
    val sentenceWordCount = sentence.split("\\s+").size
    val sentenceWithoutSpaces = sentence.replaceAll("\\s+","")
    val sentenceCharCount = sentenceWithoutSpaces.length
    println(sentenceId+" "+sentenceWithoutSpaces+" "+sentenceWordCount+" "+sentenceCharCount)
  }
}
