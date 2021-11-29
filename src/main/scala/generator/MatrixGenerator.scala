package generator

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MatrixGenerator {
  private final val MAX = 100
  type SparseRDD = RDD[(Long, Long, Long)] // (i, j, v)

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 5) {
      logger.error("Usage:\ngenerator.MatrixGenerator <i> <j> <k> <fill> <output_dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Matrix Generator").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // matrix a has dimension i * j, matrix b has dimension j * k
    val i = args(0).toLong
    val j = args(1).toLong
    val k = args(2).toLong

    // Used as probability that matrix value is non-zero
    val density = args(3).toDouble

    val output = args(4)

    val a = this.getSparseMatrix(sc, i, j, density)
    val b = this.getSparseMatrix(sc, j, k, density)

    a.saveAsTextFile(output + "a")
    b.saveAsTextFile(output + "b")
  }

  private def getSparseMatrix(sc: SparkContext, n: Long, m: Long, density: Double): SparseRDD = {
    val r = scala.util.Random
    val i = sc.range(0, n)
    val j = sc.range(0, m)
    i.cartesian(j).map {
      case (i, j) => (i, j, if (r.nextDouble() < density) r.nextInt(MAX).toLong else 0)
    }.filter {
      case (_, _, v) => v != 0
    }
  }
}
