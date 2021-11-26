package product

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparseProduct {
  type SparseRDD = RDD[(Long, Long, Long)] // (x, y, v)

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nproduct.SparseProduct <left> <right> <output_dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Sparse Product").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val leftDir = args(0)
    val rightDir = args(1)
    val output = args(2)

    val left = sc.textFile(leftDir).map(line => {
      val split = line.substring(1, line.length() - 1).split(",")
      (split(0).toLong, split(1).toLong, split(2).toLong)
    })
    val right = sc.textFile(rightDir).map(line => {
      val split = line.substring(1, line.length() - 1).split(",")
      (split(0).toLong, split(1).toLong, split(2).toLong)
    })

    val product = this.naiveSparseProduct(left, right)

    product.saveAsTextFile(output + "product")
  }

  // TODO: Block partition
  private def sparseProduct(left: SparseRDD, right: SparseRDD): SparseRDD = {
    left
  }

  // Partition col-row
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
