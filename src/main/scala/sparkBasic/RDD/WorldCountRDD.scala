package sparkBasic.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WorldCountRDD {
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create Spark Session
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("RDD-Story")
      .config("spark.executor.memory","4g")
      .config("spark.driver.memory","2g")
      .getOrCreate()

    val sc = spark.sparkContext

    val storyRDD = sc.textFile("C:/Users/kocab/Desktop/Projeler/scala_test/src/main/resources/omer_seyfettin_forsa_hikaye.txt")
    val storyRDDCount = storyRDD.count();
    println(s"Line Count : $storyRDDCount")







//    val words = storyRDD.flatMap(line => line.split(" "))
//
//
//    val wordCounts = words.map(word => (word,1)).reduceByKey((x,y) => x+y )
//
//    println(wordCounts.count())
//
//
//    wordCounts.take(10).foreach(println)
//
//
//    val mostRepeatedWords = wordCounts.map(x => (x._2, x._1))
//
//    mostRepeatedWords.take(15).foreach(println)
//
//    println("**************   EN ÇOK TEKRARLANAN KELİMELER   ***************************")
//    mostRepeatedWords.sortByKey(false).take(20).foreach(println)
  }
}
