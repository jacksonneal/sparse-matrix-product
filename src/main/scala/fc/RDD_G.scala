package fc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager

/**
 * Spark FollowerCount using RDD and groupByKey.
 */
object RDD_G_Main {

  /**
   * Main entrypoint.
   *
   * @param args - [inputDir, outputDir]
   */
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nfc.RDD_G_Main <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Follower Count").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try {
    //      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true)
    //    } catch {
    //      case _: Throwable => {}
    //    }
    // ================

    val textFile = sc.textFile(args(0))
    val counts = textFile
      .map(line => line.split(",")(1)) // access leaderId
      .filter(leader => leader.toInt % 100 == 0) // only process leaderId divisible by 100
      .map(leader => (leader, 1))
      .groupByKey()
      .map(entry => (entry._1, entry._2.count(x => true))) // process entry of (key, iterable[int])
    counts.saveAsTextFile(args(1))
  }
}
