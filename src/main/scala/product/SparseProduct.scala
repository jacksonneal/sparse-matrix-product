package product

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.:+
import scala.collection.mutable

object SparseProduct {
  private final val logger: org.apache.log4j.Logger = LogManager.getRootLogger
  final val P = 4 // # partitions, must be square
  final val HP = new HashPartitioner(P)
  type SparseRDD = RDD[(Int, Int, Long)] // (i, j, v)

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      logger.error("Usage:\nproduct.SparseProduct <n> <a_dir> <b_dir> <output_dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Sparse Product").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Both a and b matrices have dimension nxn
    val n = args(0).toInt

    val aDir = args(1)
    val bDir = args(2)
    val output = args(3)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(output), true)
    } catch {
      case _: Throwable =>
    }
    // ================

    val a = parseSparse(sc, aDir)
    val b = parseSparse(sc, bDir)

    val product = VerticalHorizontal.sparseProduct(a, b, n, sc)

    product.saveAsTextFile(output)
  }

  private def parseSparse(sc: SparkContext, dir: String): SparseRDD = {
    sc.textFile(dir).map(line => {
      val split = line.substring(1, line.length() - 1).split(",")
      (split(0).toInt, split(1).toInt, split(2).toLong)
    })
  }
}
