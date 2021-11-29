package product

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object SparseProduct {
  type SparseRDD = RDD[(Long, Long, Long)] // (i, j, v)
  private final val NUM_PARTITIONS = 10

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nproduct.SparseProduct <a_dir> <b_dir> <output_dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Sparse Product").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val aDir = args(0)
    val bDir = args(1)
    val output = args(2)

    val a = parseSparse(sc, aDir)
    val b = parseSparse(sc, bDir)

    val product = this.vhSparseProduct(a, b)

    product.saveAsTextFile(output + "product")
  }

  private def parseSparse(sc: SparkContext, dir: String): SparseRDD = {
    sc.textFile(dir).map(line => {
      val split = line.substring(1, line.length() - 1).split(",")
      (split(0).toLong, split(1).toLong, split(2).toLong)
    })
  }

  // Block partition: Naive Block Row
  private def naiveBlockRowSparseProduct(a: SparseRDD, b: SparseRDD): SparseRDD = {
    val partitioner = new HashPartitioner(NUM_PARTITIONS)
    val a_row = a.map {
      case (i, j, v) => (i, (j, v))
    }
    val b_coord = b.map {
      case (i, j, v) => ((i, j), v)
    }
    // We can either persist it, or spread it around and message pass it...
    b_coord.persist()
    a_row.partitionBy(partitioner).groupByKey().map {
      case (i: Long, vals: Iterable[(Long, Long)]) => {
        val ret = 0
        for ((j, v) <- vals) {
          ret += r.lookup((i, j)).head * v
        }
      }
    }
  }

  // Vertical-Horizontal partition
  private def vhSparseProduct(a: SparseRDD, b: SparseRDD): SparseRDD = {
    val a_col = a.map {
      case (i, j, v) => (j, (i, v))
    }
    val b_row = b.map {
      case (i, j, v) => (i, (j, v))
    }
    a_col.join(b_row).map {
      case (_, ((i, v), (j, w))) => ((i, j), v * w)
    }.reduceByKey(_ + _).map {
      case ((i, j), v) => (i, j, v)
    }
  }
}
