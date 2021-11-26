package generator

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object MatrixGenerator {
  private val MAX = 100

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 5) {
      logger.error("Usage:\ngenerator.MatrixGenerator <i> <j> <k> <fill> <output_dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Matrix Generator").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // matrixLeft has dimension i * j, matrixRight has dimension j * k
    val i = args(0)
    val j = args(1)
    val k = args(2)

    // Used as probability that matrix value is non-zero
    val fill_prob = args(3).toDouble

    val output = args(4)

    val r = scala.util.Random

    val x = sc.range(0, i.toLong)
    val y = sc.range(0, j.toLong)
    val z = sc.range(0, k.toLong)

    val left = x.cartesian(y).map {
      case (x, y) => (x, y, if (r.nextDouble() < fill_prob) r.nextInt(MAX) else 0)
    }.filter {
      case (x, y, v) => v != 0
    }

    val right = y.cartesian(z).map {
      case (x, y) => (x, y, if (r.nextDouble() < fill_prob) r.nextInt(MAX) else 0)
    }.filter {
      case (x, y, v) => v != 0
    }

    left.saveAsTextFile(output + "left")

    right.saveAsTextFile(output + "right")
  }
}
