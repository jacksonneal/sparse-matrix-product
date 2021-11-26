package generator

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}


object MatrixGenerator {
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Matrix Generator")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(args(1))
  }
}
