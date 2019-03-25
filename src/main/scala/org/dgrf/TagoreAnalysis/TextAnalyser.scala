package org.dgrf.TagoreAnalysis

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


class TextAnalyser () extends java.io.Serializable {
  private var sparkSession:SparkSession = _
  private var inputFileName:String = _

  def this (sparkSession:SparkSession,inputFileName:String) {
    this()
    this.sparkSession = sparkSession
    this.inputFileName = inputFileName

  }
  def countWords (outputFileName:String ): Unit = {


    val readFileRdd = sparkSession.sparkContext.textFile(inputFileName)
    val readFileFormatted =
      readFileRdd
        .map(s => s.replaceAll("[-]"," "))
        .map(s => s.replaceAll("[“”,।;?!\"]",""))
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
  def countWordsPerLine (outputFileName:String): Unit = {

    val readFileRdd = sparkSession.sparkContext.wholeTextFiles(inputFileName).map(s=>s._2)
    val readFileFormatted =
      readFileRdd
        .map(s => s.replaceAll("[-]"," "))
        .map(s => s.replaceAll("[“”,;\"০১২৩৪৫৬৭৮৯]",""))
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
    val sentencesStatsSchema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("sentenceWordCount", IntegerType),
      StructField("sentenceCharCount", IntegerType),
      StructField("firstTwo", StringType)
    ))
    val sentenceStatsEncoder  = RowEncoder(sentencesStatsSchema)
    val sentenceStatsDF = sentencesDF.map(row=>countWordsPerSentence(row))(sentenceStatsEncoder)

    //foreach(_.split("[।?!]"))
   /* val readFileFormatted =
      readFileRdd
        .map(s => s.replaceAll("[-]"," "))
        .map(s => s.replaceAll("[,;\"]",""))
        .map(s => s.replaceAll("[\\s+]"," "))
        .map(s=>s.trim)
        .filter(x => !x.isEmpty)*/
    sentenceStatsDF.write.mode(SaveMode.Overwrite).csv(outputFileName)
  }
  private def countWordsPerSentence (row:Row): Row = {
    val sentenceId = row.getInt(0)
    val sentence = row.getString(1)

    val sentenceWordList = sentence.split("\\s+")
    val sentenceWordCount = sentenceWordList.size
    var firstTwo = "..."
    if (sentenceWordCount >1) {
       firstTwo = sentenceWordList(0)+" "+sentenceWordList(1)+" ..."
    } else {
       firstTwo = sentenceWordList(0)+" ..."
    }

    val sentenceWithoutSpaces = sentence.replaceAll("\\s+","")
    val sentenceCharCount = sentenceWithoutSpaces.length
    Row(sentenceId,sentenceWordCount,sentenceCharCount,firstTwo.trim)
  }
}
