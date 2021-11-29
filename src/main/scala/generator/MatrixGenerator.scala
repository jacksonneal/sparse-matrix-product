package generator

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MatrixGenerator {
  private final val MAX = 100
  type SparseRDD = RDD[(Long, Long, Long)] // (i, j, v)

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\ngenerator.MatrixGenerator <n> <density> <output_dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Matrix Generator").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Both a and b matrices have dimension nxn
    val n = args(0).toLong

    // Used as probability that matrix value is non-zero
    val density = args(3).toDouble

    val output = args(4)

    val a = this.getSparseMatrix(sc, n, density)
    val b = this.getSparseMatrix(sc, n, density)

    a.saveAsTextFile(output + "a")
    b.saveAsTextFile(output + "b")
  }

  private def getSparseMatrix(sc: SparkContext, n: Long, density: Double): SparseRDD = {
    val r = scala.util.Random
    val n_range = sc.range(0, n)
    n_range.cartesian(n_range).map {
      case (i, j) => (i, j, if (r.nextDouble() < density) r.nextInt(MAX).toLong else 0)
    }.filter {
      case (_, _, v) => v != 0
    }
  }
}
