package tc

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.log4j.LogManager
import org.apache.spark.api.java.JavaRDD.fromRDD

/**
 * Spark TriangleCount using Reduce-Side join with RDD.
 */
object RS_R_Main {
  private val MAX = 12500
  private val NUM_PARTITIONS = 8

  /**
   * Main entrypoint.
   *
   * @param args - [inputDir, outputDir]
   */
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nfc.RS_R_Main <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Triangle Count")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //        val hadoopConf = new org.apache.hadoop.conf.Configuration
    //        val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //        try {
    //          hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true)
    //        } catch {
    //          case _: Throwable => {}
    //        }
    // ================

    val textFile = sc.textFile(args(0))

    // Access follower relations, filtered by MAX
    val outgoing = textFile
      .mapToPair(line => {
        val split = line.split(",")
        (split(0), split(1))
      }).filter(entry => entry._1.toInt < MAX && entry._2.toInt < MAX
    ).partitionBy(new HashPartitioner(NUM_PARTITIONS))

    // Store inverse follower relations for easy join
    val incoming = outgoing
      .mapToPair(entry => (entry._2, entry._1))

    // Store relation as key to join one hop paths with two hop paths of same endpoints
    val oneHopPaths = outgoing
      .mapToPair(entry => (entry, ""))

    // Edges (a, b) and (c, a) join as entries (a, b) (a, c) -> (b, c)
    // Indicating we have a two hop path (c, b) and need to search for one hop path (b, c)
    val revTwoHopPaths = outgoing
      .join(incoming)
      .mapToPair(entry => (entry._2, ""))

    // Search for one hop paths that complete rev two hop paths
    val counts = oneHopPaths.join(revTwoHopPaths).count() / 3

    logger.info("*******Triangle Count********")
    logger.info(counts)
    logger.info("*****************************")
  }
}
