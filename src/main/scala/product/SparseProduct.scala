package product

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object SparseProduct {
  private final val logger: org.apache.log4j.Logger = LogManager.getRootLogger
  private final val P = 10 // # partitions
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

    val a = parseSparse(sc, aDir)
    val b = parseSparse(sc, bDir)

    //    val product = this.vhSparseProduct(a, b)
    val product = this.naiveBlockRowSparseProduct(a, b, n)

    product.saveAsTextFile(output + "product")
  }

  private def parseSparse(sc: SparkContext, dir: String): SparseRDD = {
    sc.textFile(dir).map(line => {
      val split = line.substring(1, line.length() - 1).split(",")
      (split(0).toInt, split(1).toInt, split(2).toLong)
    })
  }

  class OffsetPartitioner(val numParts: Int, val numOffset: Int = 0) extends Partitioner {
    def numPartitions: Int = numParts

    def offset: Int = numOffset

    override def getPartition(key: Any): Int = (key.asInstanceOf[String].charAt(0) + offset) % numPartitions
  }

  // Block partition: Naive Block Row
  private def naiveBlockRowSparseProduct(a: SparseRDD, b: SparseRDD, n: Int): SparseRDD = {
    val hp = new HashPartitioner(P)

    // Keep a grouped by row and marked by partition
    // (p, (i, [(j, v)]))
    var a_par = a.map {
      case (i, j, v) => (i, (j, v))
    }.groupByKey(hp).mapPartitionsWithIndex {
      case (p, a_p) => a_p.map {
        case (i, a_i) =>
          // use map to track corresponding c_i
          (p, (i, a_i, new mutable.HashMap[Int, Long]()))
      }
    }

    // Group b by row
    val b_row = b.map {
      case (i, j, v) => (i, (j, v))
    }.groupByKey()

    for (p <- 0 until P) {
      // Iteratively send each row in b to a different partition, mark as such
      // (p, (i, [(j, v)]))
      val b_cur = b_row.map {
        case (i, itr) => ((i + p) % P, (i, itr))
      }

      // As a and b are marked by partition and a has partitioner, we can join them
      // map values to keep a partitioned
      a_par = a_par.join(b_cur).mapValues {
        case ((i, a_i, c_i), (j, b_j)) =>
          var a_ij = 0L
          for ((_j, v) <- a_i) {
            if (_j == j) {
              a_ij = v
            }
          }

          if (a_ij != 0L) {
            val b_j_map = new mutable.HashMap[Int, Long]()
            for ((k, v) <- b_j) {
              b_j_map(k) = v
            }

            for (k <- 0 until n) {
              val b_jk = b_j_map.get(k)
              if (b_jk.isDefined) {
                c_i(k) = c_i(k) + a_ij * b_jk.get
              }
            }
          }

          (i, a_i, c_i)
      }
    }

    a_par.flatMap {
      case (_, (i, _, c_i)) =>
        c_i.zipWithIndex.map {
          case (c_ij, j) => (i, j, c_ij)
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
