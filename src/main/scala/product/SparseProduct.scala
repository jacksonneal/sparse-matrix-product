package product

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparseProduct {
  type SparseRDD = RDD[(Long, Long, Long)] // (x, y, v)

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 5) {
      logger.error("Usage:\nproduct.SparseProduct <left> <right> <output_dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Sparse Product").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val leftDir = args(0)
    val rightDir = args(1)
    val output = args(2)

    // TODO: Load left and right as RDD/DF

    // TODO: Perform block partition product of sparse matrices

    // TODO: Write result to output
  }

  private def naiveSparseProduct(left: SparseRDD, right: SparseRDD): SparseRDD = {
    val m = left.map {
      case (x, y, v) => (y, (x, v))
    }
    val n = right.map {
      case (y, z, v) => (y, (z, v))
    }

    val product = m.join(n).map {
      case (_, ((x, v), (z, w))) => ((x, z), v * w)
    }.reduceByKey(_ + _).map {
      case ((x, z), sum) => (x, z, sum)
    }
    product
  }
}
