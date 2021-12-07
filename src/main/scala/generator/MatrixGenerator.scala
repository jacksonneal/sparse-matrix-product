package generator

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MatrixGenerator {
  private final val MAX = 10
  type SparseRDD = RDD[(Int, Int, Long)] // (i, j, v)

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\ngenerator.MatrixGenerator <n> <density> <output_dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Matrix Generator").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Both a and b matrices have dimension nxn
    val n = args(0).toInt

    // Used as probability that matrix value is non-zero
    val density = args(1).toDouble

    val output_a = args(2) + "a"
    val output_b = args(2) + "b"

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(output_a), true)
      hdfs.delete(new org.apache.hadoop.fs.Path(output_b), true)
    } catch {
      case _: Throwable =>
    }
    // ================

    val a = this.getSparseMatrix(sc, n, density)
    val b = this.getSparseMatrix(sc, n, density)

    a.coalesce(1, shuffle = true).saveAsTextFile(output_a)
    b.coalesce(1, shuffle = true).saveAsTextFile(output_b)
  }

  private def getSparseMatrix(sc: SparkContext, n: Int, density: Double): SparseRDD = {
    val r = scala.util.Random
    val n_range = sc.range(0, n)
    n_range.cartesian(n_range).map {
      case (i, j) => (i.toInt, j.toInt, if (r.nextDouble() < density) r.nextInt(MAX).toLong + 1 else 0)
    }.filter {
      case (_, _, v) => v != 0
    }
  }
}
