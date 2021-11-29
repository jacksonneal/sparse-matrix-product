package product

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparseProduct {
  type SparseRDD = RDD[(Long, Long, Long)] // (i, j, v)

  def main(args: Array[String]): Unit = {
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

    val left = parseSparse(sc, leftDir)
    val right = parseSparse(sc, rightDir)

    val product = this.vhSparseProduct(left, right)

    product.saveAsTextFile(output + "product")
  }

  private def parseSparse(sc: SparkContext, dir: String): SparseRDD = {
    sc.textFile(dir).map(line => {
      val split = line.substring(1, line.length() - 1).split(",")
      (split(0).toLong, split(1).toLong, split(2).toLong)
    })
  }

  // Block partition
  private def bbSparseProduct(left: SparseRDD, right: SparseRDD): SparseRDD = {
    left
  }

  // Vertical-Horizontal partition
  private def vhSparseProduct(left: SparseRDD, right: SparseRDD): SparseRDD = {
    val l_col = left.map {
      case (i, j, v) => (j, (i, v))
    }
    val r_row = right.map {
      case (i, j, v) => (i, (j, v))
    }
    l_col.join(r_row).map {
      case (_, ((i, v), (j, w))) => ((i, j), v * w)
    }.reduceByKey(_ + _).map {
      case ((i, j), v) => (i, j, v)
    }
  }
}
