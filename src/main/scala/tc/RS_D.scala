package tc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_unixtime, sum}

import scala.collection.mutable.ListBuffer

/**
 * Spark TriangleCount using Reduce-Side join with Dataset.
 */
object RS_D_Main {
  private val MAX = 12500

  /**
   * Main entrypoint.
   *
   * @param args - [inputDir, outputDir]
   */
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nfc.RS_D_Main <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Triangle Count")
    val sc = new SparkContext(conf)
    val session = SparkSession.builder().appName("Triangle Count").getOrCreate()
    import session.implicits._

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
    val ds = session.createDataset(textFile)

    // Access follower relations, filtered by MAX
    val outgoing = ds
      .map(line => {
        val split = line.split(",")
        (split(0), split(1))
      }).as[(String, String)]
      .filter(entry => entry._1.toInt < MAX && entry._2.toInt < MAX)

    val revTwoHopPaths = {
      // Join outgoing with rev of outgoing to get rev two hop paths
      // In rev two hop path of a->b->c, c becomes the source
      outgoing.as("out").withColumnRenamed("_2", "src")
        .join(outgoing.as("in")
          // In rev two hop path of a->b->c, a becomes the dest
          .withColumnRenamed("_1", "dest")
          .withColumnRenamed("_2", "_1"), "_1")
        // Only care about src and dest, not intermediate node
        .select("src", "dest")
        // Rename for easy join
        .withColumnRenamed("src", "_1")
        .withColumnRenamed("dest", "_2")
    }

    // Search for one hop paths that complete rev two hop paths
    val counts = outgoing.join(revTwoHopPaths, Seq("_1", "_2")).count() / 3

    logger.info("*******Triangle Count********")
    logger.info(counts)
    logger.info("*****************************")
  }
}
